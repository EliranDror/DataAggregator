import math

def entropy(string):
        "Calculates the Shannon entropy of a string"

        # get probability of chars in string
        prob = [ float(string.count(c)) / len(string) for c in dict.fromkeys(list(string)) ]

        # calculate the entropy
        entropy = - sum([ p * math.log(p) / math.log(2.0) for p in prob ])

        return entropy


string_A = "User-Agent%3a%20BOT%2f0.1%20(BOT%20for%20JCE)%0d%0aAccept%3a%20text%2fhtml,application%2fxhtml+xml,application%2fxml%3bq%3d0.9,*%2f*%3bq%3d0.8%0d%0auniqueID%3a%20CR_H8%0d%0aAccept-Language%3a%20en-US,en%3bq%3d0.5%0d%0aAccept-Encoding%3a%20gzip,%20deflate%0d%0aConnection%3a%20keep-alive%0d%0aHost%3a%20www.hmapi.com%0d%0aContent-Length%3a%200%0d%0a"
string_B = "10.2.0.1"

print(entropy(string_A))

print(entropy(string_B))
