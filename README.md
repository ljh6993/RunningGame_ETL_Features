# TrumenApp ETL Data Pipeline & Fraud Detection System

> **Note:** This repository contains a partial implementation extracted from the main codebase. Due to confidentiality agreements, only select ETL and analytics components are included for demonstration purposes.

## Overview

This project implements a real-time ETL pipeline and fraud detection system for TrumenApp, a location-based mobile gaming application. The system processes game events, performs analytics aggregations, and detects suspicious user behavior in real-time.

## Architecture Components

### 1. AWS ETL Pipeline (`aws-etl/`)
- **File:** `glue_etl_pipeline.py`
- **Purpose:** Batch processing of game events using AWS Glue and Apache Spark
- **Processes:** Location exploration, user actions, and suspicious activity events
- **Output:** Partitioned Parquet files and Druid-optimized JSON datasets

### 2. Real-time Fraud Detection (`aws-realtime/`)
- **File:** `kinesis_fraud_detector.py`
- **Purpose:** Real-time fraud detection using AWS Kinesis streams
- **Detection Methods:**
  - Impossible movement speeds (>50 km/h)
  - Teleportation detection
  - Rapid location updates (bot behavior)
  - Poor GPS accuracy patterns
  - Robotic timing patterns

### 3. Analytics Dashboard API (`dashboard-api/`)
- **File:** `app.py`
- **Purpose:** Flask API for real-time analytics dashboard
- **Features:**
  - User activity metrics
  - Feature usage analytics
  - Fraud monitoring dashboards
  - A/B test analysis
  - Geographic heatmaps

### 4. Mobile App Components (`TrumenApp/src/`)
- **Services:** Analytics, A/B Testing, and Fraud Detection service interfaces
- **Database:** Local database utilities
- **Screens:** Home screen implementations
- **Utils:** Application constants

### 5. Infrastructure (`infrastructure/`)
- **File:** `cloudformation-template.yaml`
- **Purpose:** AWS infrastructure as code template

## Demo Usage

### Prerequisites
```bash
# Install Python dependencies
pip install -r aws-realtime/requirements.txt
pip install -r dashboard-api/requirements.txt

# Set up AWS credentials
aws configure

# Install Redis (for caching and state management)
# On macOS: brew install redis
# On Ubuntu: sudo apt-get install redis-server
```

### Environment Variables
```bash
# AWS Configuration
export AWS_REGION=us-east-1
export KINESIS_STREAM_NAME=trumen-game-events
export FRAUD_ALERT_TOPIC_ARN=arn:aws:sns:us-east-1:account:fraud-alerts

# Database Configuration
export DRUID_BROKER_URL=http://localhost:8082
export REDIS_HOST=localhost
export REDIS_PORT=6379

# Pipeline Configuration
export INPUT_S3_PATH=s3://trumen-analytics-raw/events/
export OUTPUT_S3_PATH=s3://trumen-analytics-processed/
export DRUID_S3_PATH=s3://trumen-druid-deep-storage/
```

### Running the ETL Pipeline

#### 1. Batch ETL Processing
```bash
cd aws-etl/
python glue_etl_pipeline.py \
    --JOB_NAME="trumen-etl-job" \
    --INPUT_S3_PATH="s3://trumen-analytics-raw/events/" \
    --OUTPUT_S3_PATH="s3://trumen-analytics-processed/" \
    --DRUID_S3_PATH="s3://trumen-druid-deep-storage/" \
    --PROCESSING_DATE="2024-01-01"
```

#### 2. Real-time Fraud Detection
```bash
cd aws-realtime/
python kinesis_fraud_detector.py
```

#### 3. Analytics Dashboard
```bash
cd dashboard-api/
python app.py
# Access dashboard at http://localhost:5000
```

## Sample Data Structure

### Location Event
```json
{
  "event_type": "location_exploration",
  "timestamp": 1640995200000,
  "user_id": "user_12345",
  "session_id": "session_abc",
  "device_info": {
    "device_id": "device_xyz",
    "platform": "iOS",
    "app_version": "2.1.0",
    "os_version": "15.2",
    "network_type": "wifi"
  },
  "location": {
    "latitude": 37.7749,
    "longitude": -122.4194,
    "accuracy": 5.0,
    "speed": 1.5,
    "altitude": 10.0,
    "heading": 180.0
  },
  "metadata": {
    "tile_x": "1234",
    "tile_y": "5678",
    "tile_z": "15",
    "exploration_type": "walk",
    "movement_speed": "1.2"
  }
}
```

### Fraud Alert
```json
{
  "alert_type": "impossible_speed",
  "user_id": "user_12345",
  "risk_score": 85,
  "timestamp": 1640995260000,
  "details": {
    "calculated_speed_kmh": 120.5,
    "distance_km": 0.5,
    "time_diff_seconds": 15,
    "max_allowed_speed": 50
  },
  "location": [37.7749, -122.4194],
  "severity": "HIGH"
}
```

## API Endpoints

### Dashboard API
- `GET /api/health` - System health check
- `GET /api/overview` - Daily overview metrics
- `GET /api/user-activity?hours=24` - User activity trends
- `GET /api/feature-usage?hours=24` - Feature usage analytics
- `GET /api/fraud-monitoring?hours=24` - Fraud detection data
- `GET /api/ab-test/{test_name}?days=7` - A/B test results
- `GET /api/geographic-heatmap?hours=24` - Geographic heatmap data

## Key Features

### ETL Processing
- **Event Types:** Location exploration, user actions, suspicious activities
- **Aggregations:** Session metrics, daily user aggregates, exploration efficiency
- **Output Formats:** Parquet (for analysis), JSON (for Druid ingestion)
- **Partitioning:** By year/month/day/hour for efficient querying

### Fraud Detection
- **Speed Analysis:** Detects impossible movement patterns
- **Location Spoofing:** Identifies GPS accuracy anomalies
- **Bot Detection:** Recognizes automated behavior patterns
- **Real-time Alerts:** SNS integration for immediate notifications
- **Configurable Thresholds:** Adjustable detection parameters

### Analytics Dashboard
- **Real-time Metrics:** Live user activity and engagement
- **Fraud Monitoring:** Security alerts and risk analysis
- **A/B Testing:** Experiment performance tracking
- **Caching:** Redis-based result caching for performance
- **Geographic Visualization:** Heatmap of user locations

## Limitations & Confidentiality

This is a **partial implementation** for demonstration purposes. The complete system includes:
- Additional event types and processing logic
- Extended fraud detection algorithms
- Complete mobile app implementation
- Production infrastructure configurations
- Advanced analytics and machine learning models

Certain proprietary algorithms, business logic, and infrastructure details have been omitted due to contractual obligations.

## Technology Stack

- **ETL:** AWS Glue, Apache Spark, PySpark
- **Real-time Processing:** AWS Kinesis, Python asyncio
- **Analytics:** Apache Druid, Redis
- **API:** Flask, SQLAlchemy
- **Infrastructure:** AWS CloudFormation
- **Mobile:** React Native, TypeScript
- **Monitoring:** AWS SNS, CloudWatch

## License

This code is provided for demonstration purposes only and is subject to confidentiality agreements. Not for commercial use or redistribution.