# Architecture

I like an event-based approach for building systems like this. We used a similar model at Revolut, and frameworks such as [https://github.com/razz-team/eva](https://github.com/razz-team/eva) for Kotlin, built by my ex-colleagues, follow the same principle: any domain entity creation or mutation emits an event. Using the outbox pattern, the entity and its event are saved in the same transaction; a separate asynchronous process picks up and publishes the event.

This approach can be implemented with application-level event handlers backed by Kafka or another broker, either external or built into the monolith. I would not focus on the infrastructure choice here; I would focus on the product and domain architecture.

I assume that synchronisation with Instagram comments and direct messages is already implemented, as stated in the task. I also assume that retries and outages inside the existing Instagram integration are handled by that integration; here I focus on the reliability of the automation process itself.

Therefore, when I call `sendMessage`, I assume that an asynchronous delivery process is started and that the integration will later emit a `DM.Sent` or `DM.Failed` event with the original request ID.

Given this model, I would introduce a durable domain entity called `AutomationProcess`.

Its model could look roughly like this:

```text
AutomationProcess
  id
  created_at
  updated_at
  version // if we decided to use optimistic locking

  state:
    Created -> DMSent -> EmailReceived -> LinkSent | Failed

  post_id
  post_comment_id // unique by (post_comment_id), or unique by user — to discuss with business
  sent_dm_id
  dm_sent_commented_at
  conversation_id
  email

  failed_reason
  failed_reason_code
```

The flow would look roughly like this:

```text
OnEvent(InstagramPostComment.Created):
    if comment.text contains "pricing":
        CreateAutomationProcessUoW(comment)
            repo.save(
              AutomationProcess(
                state = Created, 
                post_id = comment.post_id, 
                post_comment_id = comment.id
              )
            )
```

When the process is created:

```text
OnEvent(AutomationProcess.Created):
    if process.state == Created: // ignore events for processes that have already advanced
        SendAutomationProcessDmUoW(process, request_id = process.id) // The integration deduplicates send requests by request_id, so retrying this UoW does not send another DM.
            extract author details, fail if author not found or dm is closed
            instagram.send_dm_message(...)
                
```

The `AutomationProcess.id` is used as a correlation/request ID so that later delivery events can be mapped back to the process which initiated them.

Once the DM has actually been sent:

```text
OnEvent(DM.Sent):
    process = find AutomationProcess
              where dm.request_id == process.id
              and process.state == Created

    if process != null:
        MarkAutomationProcessDmSentUoW(process, dm)
          // Idempotent: recheck state == Created inside the UoW; duplicate confirmations are a no-op.
          repo.update(
            process(state = DMSent, sent_dm_id = dm.id, conversation_id = dm.conversation_id)
          ) // emits AutomationProcess.DMSent
```

The process state change triggers the comment reply independently:

```text
OnEvent(AutomationProcess.DMSent):
    SendAutomationProcessCommentUoW(process, request_id = process.id)
      // DM sends and comment replies have separate deduplication scopes, so the same request_id can be used for both.
      instagram.send_post_comment(..., request_id = process.id) // deduplicated by request_id
```

```text
OnEvent(Comment.Sent):
    process = find AutomationProcess
              where comment.request_id == process.id

    if process != null and process.state == DMSent and process.dm_sent_commented_at == null:
        MarkAutomationProcessAsCommentSentUoW(process, comment)
          // Idempotent: if dm_sent_commented_at is already set, duplicate confirmations are a no-op.
          // optimistic lock is critical here, because dm response could be received the same time as comment posted
          repo.update(process(dm_sent_commented_at = comment.sent_at))
```


It is not required for forward execution if the event system itself is reliable, because receiving the user's email does not depend on the public comment being delivered first.

However, storing `dm_sent_commented_at` is still useful for observability, statistics, reconciliation, and recovery jobs.

Incoming direct messages are handled independently:

```text
OnEvent(DM.Received):
    process = find AutomationProcess
              where process.conversation_id == dm.conversation_id
              and process.state == DMSent

    if process != null:
        email = extractEmail(dm)

        if email != null:
            MarkAutomationProcessEmailReceivedUoW(process, email)
              // Reload and update with optimistic locking; duplicate or concurrent replies cannot advance the process twice.
              process = repo.get(process.id)
              if process.state != DMSent: return
              repo.update(process(state = EmailReceived, email = email)) // emits AutomationProcess.EmailReceived
        else:
            SendConversationMessageWeNeedEmailUoW(process, dm)
              // Deduplicated per incoming message; a new invalid reply can trigger another prompt.
              instagram.send_dm_message(
                conversation_id = process.conversation_id,
                text = "Please send your email address.",
                request_id = process.id + ":email_prompt:" + dm.id
              )
```

When the email is successfully extracted, the process moves to `EmailReceived`. Its event triggers the link DM independently:

```text
OnEvent(AutomationProcess.EmailReceived):
    if process.state == EmailReceived:
        SendMessageWithLinkUoW(process)
          // Deduplicated by request_id; the link DM uses a different key from the initial DM.
          instagram.send_dm_message(
            conversation_id = process.conversation_id,
            text = link,
            request_id = process.id + ":link"
          )
```

When that message is confirmed as sent:

```text
OnEvent(DM.Sent):
    process = find AutomationProcess
              where dm.request_id == process.id + ":link"
              and process.state == EmailReceived

    if process != null:
        MarkAutomationProcessLinkSentUoW(process)
          // Reload and update with optimistic locking; duplicate confirmations are a no-op.
          process = repo.get(process.id)
          if process.state != EmailReceived: return
          repo.update(process(state = LinkSent)) // emits AutomationProcess.LinkSent
```

The process then moves to `LinkSent`.

`OnEvent(DM.Failed)` can handle a final delivery failure; the details are outside the scope of this example.

The framework can configure UoW retries, either for a fixed number of attempts or until success within a time limit. For example, a message-send UoW can retry with backoff for up to 24 hours, reusing the same request ID to prevent duplicate sends. A successful UoW means the integration has accepted the request; actual sending is confirmed separately by `DM.Sent` or `DM.Failed`.

The important point is that `AutomationProcess` is persisted in a relational database such as PostgreSQL rather than kept in memory with something like `await message received`.

The process may wait for seconds, hours, or days, and the application may restart during that time. Persisting the state makes the workflow durable and allows it to continue from incoming domain events after a restart.

There is also a concurrency case worth handling explicitly.

For example, `Comment.Sent` and `DM.Received` may arrive at nearly the same time and both attempt to mutate the same `AutomationProcess`.

I would use optimistic locking based on the `version` field. Each UoW updates the entity only if the expected version still matches. If another event has modified the process concurrently, the UoW retries against the latest version.

Keeping `dm_sent_commented_at` separate from the main process state also makes this concurrency simpler: confirming the comment cannot accidentally move the process backwards after it has already reached `EmailReceived`.

For this take-home task, I would keep `AutomationProcess` specific to this flow rather than immediately introducing a generic workflow engine. If more automation types appear later, the same model can be generalized.
