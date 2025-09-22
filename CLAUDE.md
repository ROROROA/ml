# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is an MLOps framework built around a standardized template for model services. The project integrates several key technologies:
- **XGBoost** for machine learning models
- **MLflow** for experiment tracking and model registry
- **Feast** for feature store management
- **Ray** for distributed computing
- **Prefect** for workflow orchestration
- **Spark** for large-scale data processing
- **Kubernetes** for deployment

## Code Architecture

The project follows a modular structure:

```
ml/
├── configs/                    # Configuration files
│   ├── base.yaml              # Base project configuration
│   ├── model_params.yaml      # PyTorch model parameters
│   └── xgb_model_params.yaml  # XGBoost model parameters
├── src/
│   ├── etl/                   # Data extraction, transformation, and loading
│   ├── training/              # Model training components
│   │   ├── xgb_model.py       # XGBoost model class
│   │   └── xgb_train.py       # XGBoost training script
│   └── pipelines/             # Prefect workflow definitions
├── feature_repo/              # Feast feature definitions
└── tests/                     # Test scripts
```

### Key Components

1. **XGBoost Template**: A complete implementation of XGBoost model training with MLflow integration
2. **Prefect Workflows**: Orchestration pipelines for data processing, training, and model promotion
3. **Feast Integration**: Feature store for consistent feature engineering between training and inference
4. **Ray Distributed Training**: Scalable training capabilities
5. **Spark Processing**: Large-scale data processing capabilities

## Common Development Tasks

### Running Tests

To test the XGBoost template:
```bash
python test_xgboost_template.py
```

To run a simple Spark connectivity test:
```bash
python test_spark_simple.py
```

### Training Models

To run XGBoost training with sample data:
```bash
set PYTHONPATH=c:\Users\30630\ml
python src/training/xgb_train.py
```

### Deploying Workflows

To deploy Prefect flows:
```bash
prefect deploy
```

### Building Docker Images

To build the project Docker image:
```bash
docker build -t ml-app .
```

## Configuration

Key configuration files:
- `configs/base.yaml`: Project-level settings
- `configs/xgb_model_params.yaml`: XGBoost hyperparameters
- `prefect.yaml`: Prefect deployment configuration
- `pyproject.toml`: Python dependencies

## MLOps Workflow

1. **Feature Engineering**: Define features in `feature_repo/` and apply with Feast
2. **Data Pipeline**: Use Prefect workflows in `src/pipelines/data_pipeline.py`
3. **Model Training**: Train models using scripts in `src/training/`
4. **Model Evaluation**: Evaluate and register models with MLflow
5. **Model Deployment**: Deploy models as services using Kubernetes

## CI/CD Process

The project uses Argo Workflows and Argo CD for CI/CD:
- Argo Workflows handles CI (code changes and automatic registration)
- Argo CD manages CD (model service deployment)
- See `CICD.md` for detailed architecture