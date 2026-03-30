# Satellite Image Dataset Collection Pipeline

Large-scale dataset collection pipeline for research on deep learning-based matching algorithms considering satellite imagery characteristics.

## Project Structure
- `configs/` — Configuration files (base + mode overlays)
- `src/` — Source code modules
- `scripts/` — Pipeline entry points
- `tests/` — Unit and integration tests
- `docs/` — Documentation

## Data Storage (separate from code repo)
- `Z:\Lab_members\변상현\GEE_Dataset_copilot\runs\` — Per-run isolated outputs
- `Z:\Lab_members\변상현\GEE_Dataset_copilot\data\` — Reference data

## Quick Start
```bash
# Install dependencies
pip install -r requirements.txt

# Authenticate GEE
earthengine authenticate

# Run smoke test
python scripts/run_pipeline.py --mode-config configs/modes/smoke_test.yaml

# Run tests
python -m pytest tests/ -v
```

## Configuration
- `configs/base.yaml` — Base configuration
- `configs/modes/smoke_test.yaml` — Small-scale validation
- Mode configs are deep-merged on top of base config