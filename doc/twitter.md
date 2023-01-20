# OAuth 1.0a

You have to sign each API request by passing several generated keys and 
tokens in an authorization header. 
To start, you can generate several keys and tokens in your Twitter developer 
app’s details page, including the following:

- oauth_consumer_key - as the user name
- oauth_consumer_secret - and password for your Twitter developer app
- oauth_token - specify the Twitter account the request is made on behalf of
- oauth_token_secret - -//-

Some example with curl:

```shell
curl --request POST \
  --url 'https://api.twitter.com/1.1/statuses/update.json?status=Hello%20world' \
  --header 'authorization: OAuth \
      oauth_consumer_key="CONSUMER_API_KEY", \
      oauth_nonce="OAUTH_NONCE", \
      oauth_signature="OAUTH_SIGNATURE", \
      oauth_signature_method="HMAC-SHA1", \
      oauth_timestamp="OAUTH_TIMESTAMP", \
      oauth_token="ACCESS_TOKEN", \
      oauth_version="1.0"' \
```

# Reply Tweet

Field in_reply_to_status_id

```python
{"text": "Excited!", "reply": {"in_reply_to_tweet_id": "1455953449422516226"}}
```
