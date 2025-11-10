# apple-re
░█████████                 ░████                     ░██        ░██           ░██                          ░██    
░██     ░██               ░██                        ░██                      ░██                          ░██    
░██     ░██  ░███████  ░████████ ░██    ░██ ░██░████ ░████████  ░██ ░███████  ░████████   ░███████   ░████████    
░█████████  ░██    ░██    ░██    ░██    ░██ ░███     ░██    ░██ ░██░██        ░██    ░██ ░██    ░██ ░██    ░██    
░██   ░██   ░█████████    ░██    ░██    ░██ ░██      ░██    ░██ ░██ ░███████  ░██    ░██ ░█████████ ░██    ░██    
░██    ░██  ░██           ░██    ░██   ░███ ░██      ░███   ░██ ░██       ░██ ░██    ░██ ░██        ░██   ░███    
░██     ░██  ░███████     ░██     ░█████░██ ░██      ░██░█████  ░██ ░███████  ░██    ░██  ░███████   ░█████░██    
                                                                                                                  
                                                                                                                  
                                                                                                                  
   ░███                          ░██                 ░██████      ░██                                             
  ░██░██                         ░██                ░██   ░██     ░██                                             
 ░██  ░██  ░████████  ░████████  ░██  ░███████     ░██         ░████████  ░███████  ░██░████  ░███████            
░█████████ ░██    ░██ ░██    ░██ ░██ ░██    ░██     ░████████     ░██    ░██    ░██ ░███     ░██    ░██           
░██    ░██ ░██    ░██ ░██    ░██ ░██ ░█████████            ░██    ░██    ░██    ░██ ░██      ░█████████           
░██    ░██ ░███   ░██ ░███   ░██ ░██ ░██            ░██   ░██     ░██    ░██    ░██ ░██      ░██                  
░██    ░██ ░██░█████  ░██░█████  ░██  ░███████       ░██████       ░████  ░███████  ░██       ░███████            
           ░██        ░██                                                                                         
           ░██        ░██                                                                                         


* collect information from the multiple countries
* make informed decisions
* runs locally (well, you can fafo and deploy to your vps)

I've made and using that simple script to scan prices for refurbished apple hardware across countries – sometimes, i'm looking for a specific device which may only be available in some countries, but not mine. Also, sometimes, i can get a nice deal worth a little effort with delivery.
Example: in EU really hard to get refurbished laptop with EN keyboard; howver, it is possible to get from Ireland or UK.

# ⚠️ critical warning

1. this is vibecoded + reviwed script; it is safe for you (as long as dependencies are not compromised)
2. no responsibility, whatsoever: [Read License](LICENSE)

* thing works as of November 2025

# dependencies

this uses playwright with headless chromium underneath to load and parse pages more-less properly

# how to run

1. scrap the stores of your interest: `python3 apl.py`
	- current list of countries you will find in the `apl.py` file
	- use `python3 apl.py -h` to learn about options
	- view and modify source to your needs
2. wait. each country takes around 10 minutes to proceed.
2. view the results using _refurb-viewer.html_ page: open local json file (default name is `refurbs_by_country_playwright.json`)


# page preview screenshot

![Screenshot of a html file with opened local json after processing few countries.](/doc/screenshot.png)

