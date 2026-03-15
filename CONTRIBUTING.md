# Contributing to CodeMed AI

Thank you for your interest in contributing. This guide covers everything you need to get started.

---

## Getting Started

### 1. Fork and Clone

```bash
git clone https://github.com/YOUR_USERNAME/codemed-ai.git
cd codemed-ai
git remote add upstream https://github.com/codemedgroup/codemed-ai.git
```

### 2. Set Up Environment

```bash
python -m venv venv
source venv/bin/activate        # Windows: venv\Scripts\activate
pip install -r requirements.txt
```

### 3. Start Infrastructure

```bash
docker-compose -f docker/docker-compose.yml up -d
```

### 4. Verify Setup

```bash
# Run the full test suite — all 67 should pass
pytest tests/ -v

# Start the API
uvicorn codemed.api:app --port 8001 --reload

# Check health
curl -H "X-API-Key: dev-key-codemed" http://localhost:8001/v1/health
```

---

## Development Workflow

### Branch Naming

| Type | Pattern | Example |
|------|---------|---------|
| Feature | `feature/short-description` | `feature/batch-hcc-endpoint` |
| Bug fix | `fix/short-description` | `fix/raf-calculation-overflow` |
| Docs | `docs/short-description` | `docs/update-api-reference` |

### Commit Messages

Follow [Conventional Commits](https://www.conventionalcommits.org/):

```
feat: add batch HCC processing endpoint
fix: correct RAF delta sign when all HCCs active
docs: add FHIR integration example to README
test: add edge cases for empty ICD-10 input
refactor: extract HCC group rank logic to separate method
```

### Pull Request Process

1. Ensure all tests pass: `pytest tests/ -v`
2. Add tests for any new behaviour (maintain 67+ passing)
3. Update docstrings for any changed public methods
4. Open a PR against `main` with:
   - What the change does (1–3 sentences)
   - Why it's needed
   - How to test it

---

## Code Standards

### Python Style

- Follow PEP 8 (4-space indent, 100-char line limit for code)
- Type hints required on all public function signatures
- Docstrings required on all public classes and methods

### Testing Requirements

All new functionality must include tests. Place them in the appropriate test class in `tests/test_codemed.py`:

| Adding to... | Add tests to... |
|-------------|----------------|
| `hcc_engine.py` | `TestHCCEngine` |
| `meat_extractor.py` | `TestMEATExtractor` |
| `nlq_engine.py` | `TestNLQEngine` |
| `appeals_generator.py` | `TestAppealsGenerator` |
| `api.py` endpoints | `TestAPIModels` |
| Cross-module flows | `TestIntegrationPipeline` |

Run only your new tests during development:

```bash
pytest tests/test_codemed.py::TestHCCEngine::test_your_new_test -v
```

### HIPAA Considerations

This codebase handles Protected Health Information (PHI) in some code paths. When contributing:

- **Never** log PHI at INFO level or above (names, DOB, member IDs)
- Use placeholder/synthetic data in all test fixtures
- Append `# PHI` comment on fields that contain identifiable information
- Do not add real patient data to any fixture file

---

## Adding New Policies to the Appeals Generator

The `POLICY_INDEX` in `codemed/appeals_generator.py` contains the built-in CMS policy index. To add a new policy:

```python
"L34567": {
    "title": "Your Policy Title",
    "type": "LCD",                        # or "NCD"
    "mac": "MAC Jurisdiction Name",        # LCD only
    "effective_date": "YYYY-MM-DD",
    "icd10_covered": ["A00.0", "B00.0"],  # ICD-10 codes covered
    "cpt_covered": ["12345", "67890"],    # CPT codes covered
    "criteria": "Coverage criteria text…",
    "url": "https://www.cms.gov/medicare-coverage-database/view/lcd.aspx?lcdid=34567",
},
```

Then add a test case in `TestAppealsGenerator` to verify the citation appears in generated letters.

---

## Adding New HCC Mappings

To extend the V28 crosswalk in `codemed/hcc_engine.py`, add entries to `V28_ICD10_TO_HCC`:

```python
"X00.0": (123, "HCC Description", "HIERARCHY_GROUP", 0.250),
#          ^^^  ^^^^^^^^^^^^^^^^   ^^^^^^^^^^^^^^^    ^^^^^
#          HCC  Description        Group name         RAF weight
```

If creating a new hierarchy group, add it to `HCC_GROUP_RANKS`:

```python
"NEW_GROUP": [123, 124, 125],   # Highest severity first
```

---

## Reporting Issues

Use [GitHub Issues](https://github.com/codemedgroup/codemed-ai/issues) with the appropriate label:

| Label | Use for |
|-------|---------|
| `bug` | Something is broken or producing wrong output |
| `enhancement` | New feature or improvement request |
| `clinical-accuracy` | V28 crosswalk error, wrong RAF weight, wrong hierarchy |
| `documentation` | Unclear docs, missing examples |
| `security` | Vulnerability — please email security@codemedgroup.com instead of opening a public issue |

For clinical accuracy issues (wrong HCC mapping, incorrect RAF weight), please cite the CMS source document.

---

## Questions

Open a [Discussion](https://github.com/codemedgroup/codemed-ai/discussions) for general questions. For security issues, email security@codemedgroup.com directly.
