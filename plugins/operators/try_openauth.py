# import time
# from airflow.models import BaseOperator
# import requests
# from flask import Flask, request
# from urllib.parse import urlencode

# class TryOpenAuthOperator(BaseOperator):
#     """
#     Try Open Authentication

#     """

#     def __init__(
#         self,
#         *args,
#         **kwargs
#     ):
#         super().__init__(*args, **kwargs)


#     def execute(self, context):
#         # Get Hook class
#         # url = "https://login.microsoftonline.com/6f70cc55-b28b-418f-aa5c-1381913699bb/oauth2/authorize?client_id=10ab73d2-ce2b-4935-852d-ba226030dfc6&response_type=code&response_mode=query&scope=Notebook.Execute.All Notebook.Read.All Notebook.ReadWrite.All  Notebook.Reshare.All&state=12345&nonce=7362CAEA-9CA5-4B43-9BA3-34D7C303EBA7&redirect_uri=http://localhost"

#         # payload = {}
#         # files={}

#         # response = requests.request("GET", url, data=payload, files=files)

#         # print(response.text)

#         # Start a local Flask server to handle the OAuth callback
#         app = Flask(__name__)

#         @app.route('/oauth/callback')
#         def oauth_callback():
#             # Retrieve the authorization code from the query parameters
#             auth_code = request.args.get('code')
#             return f'Authorization Code: {auth_code}'

#         # Run the Flask server in a separate thread
#         import threading
#         thread = threading.Thread(target=lambda: app.run(host='localhost', port=8080))
#         thread.start()

#         # Wait briefly for the server to start
#         time.sleep(1)

#         # Construct the OAuth authorization URL
#         authorization_url = (
#             "https://login.microsoftonline.com/6f70cc55-b28b-418f-aa5c-1381913699bb/oauth2/authorize?"
#             "client_id=10ab73d2-ce2b-4935-852d-ba226030dfc6&"
#             "response_type=code&"
#             "response_mode=query&"
#             "scope=Notebook.Execute.All Notebook.Read.All Notebook.ReadWrite.All Notebook.Reshare.All&"
#             "state=12345&nonce=7362CAEA-9CA5-4B43-9BA3-34D7C303EBA7&"
#             "redirect_uri=https://www.google.com"
#         )

#         # Open the authorization URL in a browser for the user to authenticate
#         import webbrowser
#         webbrowser.open(authorization_url)

#         # You can continue your workflow logic here, waiting for the auth_code asynchronously.
#         # # For demonstration, let's just pause execution.
#         # input("Press Enter to continue after authorization...")

#         # You can now use the obtained auth_code for further authentication steps

#         # Stop the Flask server
#         func = request.environ.get('werkzeug.server.shutdown')
#         if func:
#             func()

#         # You may want to handle the case where the user didn't authorize your app.
#         return 'OAuth process completed.'
