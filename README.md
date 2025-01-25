# pyews
## Overview

This repository combines the functionalities of Ecowitt and pywws to provide a comprehensive solution for weather data collection and analysis.

## Ecowitt

Ecowitt is a popular weather station that provides various sensors to measure different weather parameters such as temperature, humidity, wind speed, and more. The data collected by Ecowitt devices can be accessed and processed for further analysis.

## pywws

pywws is a Python library for reading and processing weather data from weather stations. It supports various weather stations and provides tools to analyze and visualize the collected data.

## Installation

To install the necessary dependencies, run the following command:

```bash
pip install -r requirements.txt
```

## Usage

To use this repository, follow these steps:

Clone the repository and navigate to the project directory.

Create a copy of the `.env.example` file and rename it to `.env`.

Fill in the required environment variables in the .env file.

Run the Flask application:

```bash
python3 app.py
```

This will start the server and process incoming data from your Ecowitt weather station.

## Contributing

Contributions are welcome! Please open an issue or submit a pull request for any improvements or bug fixes.

## License

This project is licensed under the MIT License.
