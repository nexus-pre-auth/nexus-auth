# NexusAuth AI: Prior-Authorization Automation Engine

NexusAuth AI is an intelligent platform designed to automate the prior-authorization process for healthcare providers, with a specialized focus on WebPT integration and denial recovery. The system streamlines workflows by connecting directly to WebPT, syncing claims, and using an AI-powered engine to identify and process claim denials.

## Key Features

*   **WebPT OAuth Integration**: Securely connects to WebPT to access and synchronize patient and claim data.
*   **Automated Claim Syncing**: Keeps claim information up-to-date, providing a real-time view of claim statuses.
*   **Denial Detection & Recovery**: Intelligently identifies denied claims (specifically targeting denial codes like CO-16, CO-50, and CO-97) and facilitates the recovery process.
*   **Revenue Tracking**: Provides insights into revenue cycles and the financial impact of claim denials.
*   **CMS Policy Ingestion**: Stays current with the latest CMS policies by automatically ingesting and processing regulatory updates.
*   **AI-Powered Tagging**: Uses OpenAI embeddings to tag and categorize clinical data, enhancing the accuracy of the denial analysis.

## Tech Stack

*   **Backend**: Flask, Python
*   **Database**: PostgreSQL
*   **Caching & Task Queuing**: Redis, RQ (Redis Queue)
*   **AI & Embeddings**: OpenAI
*   **Integration**: WebPT OAuth API

## Project Structure

The repository is organized into the following key modules:

```
/
├── app.py                  # Main Flask application entry point
├── api_routes.py           # API endpoint definitions
├── denial_recovery.py      # Core logic for denial processing
├── revenue_tracking.py     # Revenue and financial tracking module
├── webpt/                    # Modules for WebPT integration
│   ├── oauth.py            # WebPT OAuth2 connection handling
│   └── sync.py             # Data synchronization with WebPT
├── ingestion/              # Data ingestion and processing pipeline
│   ├── pipeline.py         # Main ingestion pipeline
│   ├── embedder.py         # Content embedding using OpenAI
│   └── scrapers/           # Scrapers for external data (e.g., CMS)
├── database/               # Database schemas and migrations
├── tests/                  # Unit and integration tests
└── requirements.txt        # Python dependencies
```

## Getting Started

### Prerequisites

*   Python 3.9+
*   PostgreSQL
*   Redis
*   Docker (optional, for containerized setup)

### Installation

1.  **Clone the repository:**
    ```bash
    git clone https://github.com/nexus-pre-auth/nexus-auth.git
    cd nexus-auth
    ```

2.  **Install Python dependencies:**
    ```bash
    pip install -r requirements.txt
    ```

3.  **Set up the database:**
    - Create a PostgreSQL database.
    - Apply the database migrations located in the `/database/migrations` directory.

4.  **Configure environment variables:**
    - Set up the necessary environment variables for the Flask application, including database credentials, Redis connection details, and API keys for OpenAI and WebPT.

### Running the Application

```bash
flask run
```

## License

This project is licensed under the terms of the [MIT License](LICENSE).
