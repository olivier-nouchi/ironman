# Introduction

This project was born out of curiosity. Not any statistics can be found on the internet on IronMan and triathlons in
general. It aims at understanding further the performance of athletes and mine insightful findings.


# Installation

1. Download the project or clone the project
2. You can download the needed packages for this project through:

```bash
pip install -r requirements.txt
```


This project was developped with python 3.8

# Usage
In the `input` directory create a json file named `user_config.json` that contains the following keys:
- "user_local_db_password": password to access your local database (__string__).
- "wtc_priv_key": private key to access the IronMan API (__string__). You can find it in the Network section of the
developper tools, "Headers" section, "Request Headers".
  

# Notes
- If the scraping stops on the way, it will automatically resume back where it stopped. It won't scrape events that it
has already scraped.

## License
MIT License (please refer to LICENSE.txt)