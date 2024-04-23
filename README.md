# PhonePe Data Analysis Project

## Overview

The PhonePe Data Analysis Project aims to analyze the data collected from the PhonePe application to gain insights into transaction patterns, user behavior, and performance metrics over time. The project involves data visualization, statistical analysis, and querying a MySQL database.

## Functionality

- **Data Visualization**: The project provides an interactive interface built using Streamlit, allowing users to explore various analysis questions and visualize the data through charts and graphs.

- **Statistical Analysis**: Statistical methods are employed to derive meaningful insights from the data, such as trends over time, correlations between different metrics, and comparative analysis of user behavior.

- **Database Querying**: The application queries a MySQL database to retrieve relevant data for analysis. SQL queries are used to filter, aggregate, and manipulate the data as required.

## Architecture

The architecture of the PhonePe Data Analysis Project can be described as follows:

- **Frontend**: The Streamlit application serves as the frontend, providing a user-friendly interface for interaction and visualization of data.

- **Backend**: The backend consists of Python scripts responsible for data processing, statistical analysis, and database querying.

- **Data Storage**: The dataset used in the analysis is stored in a MySQL database, which stores transactional data, user data, and other relevant metrics collected from the PhonePe application.

## Data Flow

1. **Data Retrieval**: The Streamlit application sends requests to the backend to retrieve data from the MySQL database based on user-selected analysis questions.

2. **Data Processing**: The backend processes the retrieved data using Pandas and performs necessary transformations, cleaning, and aggregations to prepare it for visualization and analysis.

3. **Statistical Analysis**: Statistical methods and calculations are applied to the processed data to derive insights and generate meaningful visualizations.

4. **Visualization**: The processed data is visualized using Plotly, a Python library for interactive data visualization, and displayed to the user through the Streamlit interface.

## Usage

1. **Installation**: Clone the repository and install the required dependencies using `pip install -r requirements.txt`.

2. **Database Setup**: Ensure you have a MySQL database set up with the required schema and data imported.

3. **Running the Application**: Start the Streamlit application using `streamlit run app.py`.

4. **Interacting with the Application**: Access the application in your web browser and select different analysis questions from the dropdown menu. Click "Get Answer" to see the insights and visualizations generated based on the selected question.

## Data Sources

- **PhonePe Dataset**: Contains transactional data, user data, and other relevant metrics collected from the PhonePe application.

## Dependencies

- Python 3.x
- Streamlit
- Pandas
- Plotly
- MySQL Connector/Python

