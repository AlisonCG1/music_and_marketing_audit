import googleapiclient.discovery
API_KEY = "AIzaSyAlqqfNMR7opsvEx2TULLN4ljnVbZXV-_s"
youtube = googleapiclient.discovery.build("youtube", "v3", developerKey=API_KEY)
try:
    response = youtube.videos().list(
        part="snippet,contentDetails,statistics",
        id="Eb8rXCzJMUc"
    ).execute()
    print(response)
except Exception as e:
    print(f"Error: {e}")