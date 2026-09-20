# MeeshoPoster

Telegram bot: send a Meesho product link -> get screenshot + short caption + @price + link.

## Setup

```powershell
cd d:\python\Telegrambots\MeeshoPoster
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
playwright install chromium
copy .env.example .env
# edit .env with real values
```

## .env

- `API_ID`, `API_HASH` — from https://my.telegram.org
- `BOT_TOKEN` — from @BotFather

## Run

```powershell
python bot.py
```

## Notes

- Uses mobile UA + headless Chromium. Meesho's SPA may take a few seconds to hydrate (4s wait built-in).
- Caption is capped at 90 chars to stay minimal.
- Output format:
  ```
  <short title>

  @₹<price>
  https://meesho.com/...
  ```
