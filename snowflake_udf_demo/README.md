# Skyflow for Snowflake Setup Instructions

Pre-requisites:
* A Snowflake account with Account Admin privileges
* A Skyflow account with Account Admin privileges - Contact Skyflow [here](https://www.skyflow.com/contact-sales) to get one!
* A command-line terminal with python installed

Setup Steps:
1. Download this repo, and navigate to the ```snowflake_udf_demo``` folder in a command-line terminal.

2. Log into Skyflow via browser and [create a service account](https://docs.skyflow.com/api-authentication/#create-a-service-account). Ensure the Account Admin role is enabled when going through the creation wizard. Move or copy the auto-downloaded ```credentials.json``` file into the ```snowflake_udf_demo``` folder.

3. Within Skyflow, click the settings gear icon on the left. Copy your Account ID and paste it into the ```vault_details.cfg``` file.

4. In your terminal window, run the command: ```python script_setup.py``` or ```python3 script_setup.py``` (depending on your python version). This 

3. Log into Snowflake and click ```Create Worksheet from SQL File``` in the top-right three-dot menu. Select and Import the ```skyflow_udf_setup.sql``` file. Repeat for the ```skyflow_udf_demo.sql``` file.

4. Open the imported `skyflow_udf_setup.sql` worksheet and ensure you are using the Snowflake ACCOUNT_ADMIN role. Execute the worksheet in full using the ```Run All``` option in the Run command button drop-down (top-right). If you receive the message ```Function SKYFLOW_PROCESS_PII successfully created.```, the setup is successful!

5. Switch to the imported ```skyflow_udf_demo.sql``` worksheet, and again ensure you are using the Snowflake ACCOUNT_ADMIN role. Proceed line by line through the demo script. Prior to running the tokenize table step, update the last parameter with your email address. Note that this email address must correspond to an existing Skyflow Studio user in the relevant account. Also: Ensure that vault name remains "SkyflowVault".

6. Upon running the tokenize table step, you can refresh your Skyflow Studio web page - there will be a new vault and table in Skyflow securely hosuing your Snowflake PII data! Continue through the demo steps accordingly.

--This concludes the demo. To reset the Snowflake portion of the demo, import the ```skyflow_udf_reset.sql``` file as a worksheet and execute. You can then start again from the ```Run Demo``` portion of these instructions. Thank you!--