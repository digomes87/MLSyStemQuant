# QuantML - Real-Time ETF NAV Monitoring System

QuantML is a high-performance, event-driven data engineering platform designed to calculate and monitor the Net Asset Value (NAV) of ETFs (Exchange Traded Funds) in real-time. By processing market data streams with low latency, it provides traders and portfolio managers with instant insights into fair value discrepancies, enabling arbitrage strategies and effective risk management.

## Key Features

- **Real-Time Processing**: Ingests market data and calculates NAV updates with sub-second latency using Apache Kafka.
- **Event-Driven Architecture**: Decoupled microservices for scalability and fault tolerance.
- **Live Dashboard**: Interactive Streamlit visualization for monitoring NAV trends and market components.
- **Data Persistence**: Automated ETL pipeline to archive historical data to Data Lake (S3) and Data Warehouse (Snowflake) - _Mocked for local development_.
- **Robust CI/CD**: Automated testing, linting, and security scanning with GitHub Actions.

## Architecture

The system follows a microservices architecture orchestrated by Docker Compose:

```mermaid
graph LR
    P[Producer] -->|Market Data| K[Kafka]
    K -->|Market Data| C[Calculator]
    C -->|NAV Updates| K
    K -->|NAV Updates| E[ETL Service]
    K -->|NAV Updates| D[Dashboard]
    E -->|JSON/Parquet| S[Storage (S3/Snowflake)]
    D -->|UI| U[User]
```

### Components

| Service            | Description                                                                              | Tech Stack                                |
| ------------------ | ---------------------------------------------------------------------------------------- | ----------------------------------------- |
| **Producer**       | Simulates real-time market data (stock prices, FX rates) for ETF constituents.           | Python, Confluent Kafka                   |
| **Calculator**     | Consumes market data, applies ETF basket logic, and publishes NAV updates.               | Python, Confluent Kafka                   |
| **ETL**            | Subscribes to NAV updates and batches them for persistent storage (Data Lake/Warehouse). | Python, Boto3 (Mock), Snowflake Connector |
| **Dashboard**      | Visualizes real-time NAV, component prices, and system health.                           | Streamlit, Python                         |
| **Infrastructure** | Message broker and coordination.                                                         | Apache Kafka, Zookeeper                   |

## Getting Started

### Prerequisites

- **Docker** and **Docker Compose** installed.
- **Python 3.10+** (for local development).
- **Git**.

### Installation

1. **Clone the repository:**

   ```bash
   git clone https://github.com/your-org/QuantML.git
   cd QuantML
   ```

2. **Configure Environment:**
   The project comes with default configuration for local development. A `.env` file can be created to override defaults.

### Running the Application

Start the entire stack with a single command:

```bash
docker compose up --build -d

or in the old command

docker-compose up --build -d
```

This will launch:

- Zookeeper & Kafka (Messaging Backbone)
- Producer (Data Generator)
- Calculator (Business Logic)
- ETL (Data Ingestion)
- Dashboard (Visualization)

> **Note:** The first run may take a few minutes to download images and install dependencies.

### Accessing the Dashboard

Once running, open your browser and navigate to:
**[http://localhost:8501](http://localhost:8501)**

## Project Structure

```
QuantML/
├── .github/              # CI/CD Workflows (GitHub Actions)
├── calculator/           # NAV Calculation Service
├── common/               # Shared Libraries (Kafka Wrappers)
├── dashboard/            # Streamlit Visualization App
├── data/                 # Local storage for ETL output (Mock S3)
├── docs/                 # Documentation (CI/CD Process)
├── etl/                  # ETL Service (S3/Snowflake Ingestion)
├── producer/             # Market Data Generator
├── docker-compose.yml    # Container Orchestration
└── requirements.txt      # Project Dependencies
```

## Configuration

The system uses environment variables for configuration. Key variables in `docker-compose.yml`:

- `KAFKA_BOOTSTRAP_SERVERS`: Address of the Kafka broker.
- `KAFKA_TOPIC_MARKET_DATA`: Topic for raw market data.
- `KAFKA_TOPIC_ETF_NAV`: Topic for calculated NAVs.
- `AWS_ACCESS_KEY_ID`: Set to `mock_access_key` to enable local file storage instead of S3.

## Development & Quality Assurance

We follow strict engineering practices to ensure reliability.

### Running Tests - Almost, I don't criated this, is for the future

Unit tests can be run using `pytest`:

```bash
pytest
```

### CI/CD Pipeline

Spend a lot time for configure but save time after

Every commit to `main` or `develop` triggers our automated pipeline, which includes:

My loved command - ruff format . && ruff check .

1. **Linting**: Code style checks with `Ruff`.
2. **Security**: Vulnerability scanning with `Bandit`.
3. **Testing**: Unit tests with `Pytest`.
4. **Auto-PR**: Automatic Pull Request creation for feature branches.

For more details, see [CI/CD Process Documentation](docs/CI_CD_PROCESS.md).

---

_Built by the QuantML Data Engineering Team._
