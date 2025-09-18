# s3tool

Small CLI for common S3 tasks:

- Bucket summary: total object count + total bytes (optionally under a prefix)
- Download an object by path (key)
- Upload a file to a given bucket/key

## Install

```bash
# in project root
python -m venv .venv && source .venv/bin/activate
pip install -U pip
pip install .
# or editable dev install
pip install -e .
```

Show help
```bash
s3tool --help
```

Summarize whole bucket
```bash
s3tool summary my-bucket
```

Summarize under a prefix (fast if your data is structured)
```bash
s3tool summary my-bucket --prefix logs/2025/ --json
```

Download an object
```bash
s3tool download my-bucket path/to/object.txt ./downloads/object.txt
```

Upload a file
```bash
s3tool upload ./report.json my-bucket reports/2025-09/report.json --content-type application/json
```

With a specific profile & region
```bash
s3tool --profile myprofile --region eu-central-1 summary my-bucket
```