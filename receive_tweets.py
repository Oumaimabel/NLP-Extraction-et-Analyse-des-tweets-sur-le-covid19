# Partie 1 : Spark Streaming &amp; API Twitter ( covid19 ) : receive_tweets.py


import tweepy
from tweepy.auth import OAuthHandler
from tweepy import Stream
from tweepy.streaming import StreamListener
import socket # Le module socket est requis pour communiquer avec l'API Twitter sur notre machine locale. 
import json # Pour travailler avec des objets json, nous aurons besoin du module json.

# Nos clés d"identification
consumer_key='IsNmYLpt3SrvWN3EoiKheA1JJ'
consumer_secret='ZI6tUVTBnB3aVFiQWQXGnxzYndHcOmD2S0I7sfQEk6Ct7N7oNn'
access_token='1476928546404806658-KcF0D70h8c8PAnsHygmY1mMidhqFNJ'
access_secret='qwEPD5kPxYC36wxPFkHh873pE7vPKiZA9fTHrWAE2YG7s'


#TweetListener représente une instance de StreamListener qui se connecte à l'API Twitter et renvoie un tweet à la fois. Dès que nous activons le Stream, les instances sont automatiquement créées.

#La classe TweetsListener contient trois méthodes : on_data, if_error et __init__. La méthode on_data récupère le fichier json du tweet entrant, qui contient un tweet, et détermine quelles parties du tweet doivent être conservées. Par exemple, le message, les commentaires ou les hashtags d'un tweet. If_error s'assure que le flux fonctionne et __init__ configure le socket de l'API Twitter.

class TweetsListener(StreamListener):

    def __init__(self, csocket):
        self.client_socket = csocket
    # we override the on_data() function in StreamListener
    def on_data(self, data):
        try:
            message = json.loads( data )
            print( message['text'].encode('utf-8') )
            self.client_socket.send( message['text'].encode('utf-8') )
            return True
        except BaseException as e:
            print("Error on_data: %s" % str(e))
        return True

    def if_error(self, status):
        print(status)
        return True


def send_tweets(c_socket):
      # Afin de récupérer les données de l'API Twitter, nous devons d'abord authentifier notre connexion à l'aide des informations d'identification prédéfinies. Après l'authentification, nous diffusons les objets de données tweet contenant un mot-clé. TweetListener renvoie les tweets sous forme d'objets.
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_secret)
    # commencer à envoyer des données depuis l'API Streaming
    twitter_stream = Stream(auth, TweetsListener(c_socket))
    twitter_stream.filter(track=['covid19']) #Nous avons choisi le thème du covid19
    
    
    
    
    #Le streaming des données de l'API Twitter nécessite la création d'un socket TCP d'écoute sur la machine locale (serveur) sur une adresse  IP et un port locaux prédéfinis.
    # Un socket se compose d'un côté serveur, qui est notre machine locale, et d'un côté client, qui est l'API Twitter. Le socket ouvert côté serveur écoute les connexions du client. Lorsque le côté client est opérationnel, le socket reçoit les données de l'API Twitter en fonction du sujet ou des mots-clés définis dans l'instance StreamListene
if __name__ == "__main__":
    new_skt = socket.socket()         # initiate a socket object
    host = "192.168.1.102"     # local machine address
    port = 5555                # specific port for your service.
    new_skt.bind((host, port))        # Binding host and port

    print("Now listening on port: %s" % str(port))  

    new_skt.listen(5)                 #  waiting for client connection.
    c, addr = new_skt.accept()        # Establish connection with client. it returns first a socket object,c, and the address bound to the socket

    print("Received request from: " + str(addr))
    # and after accepting the connection, we aill sent the tweets through the socket
    send_tweets(c)
    
    

    