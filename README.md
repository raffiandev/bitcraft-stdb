# SpacetimeDB websocket reader

Forked Bitjita Bitcraft code. This is IN PROGRESS for experimentation and is not clean by any means!!

## Setup

Install uv [github link](https://github.com/astral-sh/uv) 

```bash
git clone https://github.com/raffdev/bitcraft-stdb
uv venv --python 3.12 --seed
source .venv/bin/activate
uv add requests websockets python-dotenv
touch .env
```

Add your env vars to .env:

MY_EMAIL="youremail@yourdomain.com"
BASE_API="https://api.bitcraftonline.com/authentication"

run `uv run auth.py` to send an access code to your email, then paste the token into your _AUTH key with Bearer in front

Add remaining vars:

BITCRAFT_SPACETIME_HOST="bitcraft-early-access.spacetimedb.com"
BITCRAFT_SPACETIME_AUTH="Bearer ey...."

NOTE: If you are logged into the game any further commands may disconnect you.

## Usage

View Chat: `uv run chat.py`

Download tables: `uv run bitjita_dump.py`

