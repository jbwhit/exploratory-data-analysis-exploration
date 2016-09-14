# Notes

Following most of the instructions here: http://airbnb.io/caravel/installation.html

 - Python 2.7
 
```bash
source activate py2
conda install cryptography
pip install caravel
```

Looks like the `pip install` grabs a bunch of other library versions.

```
# Create an admin user
fabmanager create-admin --app caravel

# Initialize the database
caravel db upgrade

# Create default roles and permissions
caravel init

# Load some data to play with
caravel load_examples

# Start the web server on port 8088
caravel runserver -p 8088

```

Sure enough, http://localhost:8088/caravel/dashboard/world_health/ works!


