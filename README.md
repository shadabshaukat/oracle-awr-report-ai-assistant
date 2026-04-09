# Oracle AWR Report Analyzer (File-Based, Zero DB Access)

AI-powered Oracle report analysis tool for AWR/SQL/log/CSV/HTML artifacts with:
- ✅ No direct DB connection
- ✅ No DB credentials
- ✅ File upload only workflow
- ✅ Multi-LLM backend: **OCI GenAI**, **AWS Bedrock**, **Local Ollama**

## Features

- 10 analysis modules (load profile, top SQL, waits, IO, memory, security, etc.)
- Actionable recommendations with priority and traffic-light severity (🔴🟡🟢)
- Dual execution modes:
  - **AI Analysis mode**: LLM reads uploaded artifacts and generates contextual recommendations
  - **Non-AI Analysis mode**: deterministic, rule-based parsing of known AWR/wait/error patterns
- Optimized for enterprise-safe, CISO-friendly adoption
- Configurable provider/model via `.env`

## Quick Start (Local)

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
python run.py
```

Open: `http://localhost:8080`

## Docker (Production-like)

```bash
cp .env.example .env
docker build -t awr-genai-assistant .
docker run --rm -p 8080:8080 --env-file .env awr-genai-assistant
```

The app binds to **0.0.0.0:8080** using Gunicorn in the container.

## Provider Configuration

Set in `.env`:

- `LLM_PROVIDER=ollama` (or `bedrock`, `oci`)
- `LLM_MODEL=...`

### Ollama
- Ensure Ollama is running and model is pulled
- In Docker on macOS: `OLLAMA_URL=http://host.docker.internal:11434`

### AWS Bedrock
- Set `AWS_REGION`, `BEDROCK_MODEL_ID`
- Provide AWS credentials through environment/IAM

### OCI Generative AI
- Set `OCI_COMPARTMENT_ID`, `OCI_MODEL_ID`, `OCI_GENAI_ENDPOINT`
- Mount OCI config if running in Docker, e.g.:

```bash
docker run --rm -p 8080:8080 \
  --env-file .env \
  -v /Users/shadab/.oci:/root/.oci:ro \
  awr-genai-assistant
```

## Supported Upload Types

- `.html`, `.htm`, `.txt`, `.csv`, `.log`, `.sql`

## Security Posture

- No database network calls
- No database credential handling
- Uploaded files are processed then removed

## Notes

- AI mode requires a configured model provider (Ollama/Bedrock/OCI).
- Non-AI mode runs without any LLM dependency and provides deterministic baseline findings.
