# AI Sales Channels

## Entry Points

- `python AI_BOT/main.py` - Telegram sales bot
- `python AI_BOT/vk_main.py` - VK long poll sales bot
- `python AI_BOT/max_main.py` - MAX long poll sales bot

## Shared Flow

All channels use the same lead pipeline:

1. Receive inbound message
2. Save/update local lead
3. Sync current lead state to Google Sheets
4. Generate AI reply or pause in manager mode
5. Save outbound reply and sync the lead again

## VK

Required `.env` keys:

- `VK_LONGPOLL_TOKEN`
- `VK_GROUP_ID`

Optional:

- `VK_ACCESS_TOKEN` or `VK_API_KEY` for wall posting scripts
- `VK_API_VERSION` (default: `5.199`)
- `VK_LONGPOLL_WAIT` (default: `25`)

Notes:

- The bot uses VK Bot Long Poll via `groups.getLongPollServer`.
- Incoming `message_new` events are processed as leads.
- Replies are sent back through `messages.send`.

## MAX

Required `.env` keys:

- `MAX_BOT_TOKEN`

Optional:

- `MAX_LONGPOLL_TIMEOUT` (default: `30`)
- `MAX_LONGPOLL_LIMIT` (default: `100`)

Notes:

- The bot polls `GET /updates` for `message_created` events.
- Replies are sent through `POST /messages` using `user_id` or `chat_id`.

## Admin Notifications

If `AI_SALES_ADMIN_ID` and `TELEGRAM_BOT_TOKEN` are configured, inbound messages from all channels are mirrored to the admin in Telegram.
