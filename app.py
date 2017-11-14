from flask import Flask, render_template, request, redirect, url_for, jsonify
from slackclient import SlackClient
from cachetools import TTLCache
from datetime import datetime, timedelta

from logic import create_dataframe, process_dataframe, get_restaurants_by_average_time, get_n_most_popular_restaurants, \
  get_n_earliest_restaurants, get_n_latest_restaurants, get_average_time_for_restaurant

import os
import json

app = Flask(__name__)

BOT_ACCESS_TOKEN = os.environ.get('BOT_ACCESS_TOKEN')
OAUTH_ACCESS_TOKEN = os.environ.get('OAUTH_ACCESS_TOKEN')
VERIFICATION_TOKEN = os.environ.get('VERIFICATION_TOKEN')

slack_client = SlackClient(BOT_ACCESS_TOKEN)
cache = TTLCache(maxsize=1, ttl=60*60*24)

@app.route('/slack_event', methods=['POST'])
def slack_event():
  print('slack_event endpoint triggered')
  req = json.loads(request.data)
  if req.get('token') == VERIFICATION_TOKEN:
    print('Token verified')
    if 'challenge' in req:
      print('Challenge')
      return req.get('challenge')
    event = req.get('event')
    print('Event is - ' + json.dumps(event))
    if not event.get('bot_id') and 'text' in event:
      print('Generate interactive message')
      attachments = [
        {
          "text": "Choose one of the following options",
          "fallback": "You are unable to choose an option",
          "callback_id": "foodbot_selection",
          "color": "#3AA3E3",
          "attachment_type": "default",
          "actions": [
            {
              "name": "foodbot",
              "text": "Most Popular",
              "type": "button",
              "value": "popular"
            },
            {
              "name": "foodbot",
              "text": "Arrives Earliest",
              "type": "button",
              "value": "earliest"
            },
            {
              "name": "foodbot",
              "text": "Arrives Latest",
              "type": "button",
              "value": "latest"
            },
            {
              "name": "foodbot",
              "text": "Before 12:30",
              "type": "button",
              "value": "first_half"
            },
            # {
            #   "name": "foodbot",
            #   "text": "After 12:30",
            #   "type": "button",
            #   "value": "second_half"
            # },
            {
              "name": "foodbot",
              "text": "Select a restaurant to get the average delivery time",
              "type": "select",
              "data_source": "external",
              "min_query_length": 2
            }
          ]
        }
      ]
      print('Posting message to channel ' + event.get('channel'))
      slack_client.api_call(
        'chat.postMessage',
        channel=event.get('channel'),
        text='',
        attachments=json.dumps(attachments)
      )
      print('Message posted')
    return ''

@app.route('/slack_options', methods=['POST'])
def slack_options():
    restaurants = get_dataframe()['restaurant'].values
    restaurants_json = []
    for restaurant in restaurants:
      restaurants_json.append({ 'text': restaurant, 'value': restaurant })
    return jsonify({ 'options': restaurants_json })

@app.route('/slack_action', methods=['POST'])
def slack_action():
  print('slack_action endpoint triggered')
  form_json = json.loads(request.form['payload'])
  if form_json.get('token') == VERIFICATION_TOKEN:
    print('Token verified')
    selection = form_json.get('actions')[0]
    if 'selected_options' in selection:
      selection = selection.get('selected_options')[0]
    selection = selection.get('value')
    print('Selection is - ' + selection)
    channel = form_json.get('channel').get('id')
    print('Updating channel ' + channel)
    slack_client.api_call(
      'chat.update',
      channel=channel,
      ts=form_json.get('message_ts'),
      text=get_results_from_selection(selection=selection),
      attachments=[]
    )
    print('Channel updated')
  return ''

@app.route('/debug', methods=['GET'])
def debug():
  print('debug endpoint triggered')
  return 'debug'

def read_from_channel(count=1000, days_back=90):
  now = datetime.now()
  then = now - timedelta(days=days_back)
  messages = get_messages(count=count, oldest=then.strftime('%s'))
  print('Creating dataframe')
  dataframe = create_dataframe(messages=messages)
  print('Processing dataframe')
  grouped_dataframe = process_dataframe(dataframe=dataframe)
  print('Data ready')
  return grouped_dataframe

def get_dataframe():
  messages = None
  try:
    messages = cache['messages']
    print(str(len(messages)) + ' messages in cache')
  except KeyError:
    print('Messages not in cache')
    messages = read_from_channel()
    cache['messages'] = messages
  return messages

def get_results_from_selection(selection):
  grouped_dataframe = get_dataframe()
  if selection == 'popular':
    return get_n_most_popular_restaurants(dataframe=grouped_dataframe)
  elif selection == 'earliest':
    return get_n_earliest_restaurants(dataframe=grouped_dataframe)
  elif selection == 'latest':
    return get_n_latest_restaurants(dataframe=grouped_dataframe)
  elif selection == 'first_half':
    return get_restaurants_by_average_time(dataframe=grouped_dataframe, start_hour='11:00', end_hour='12:30')
  # elif selection == 'second_half':
  #   return get_restaurants_by_average_time(dataframe=grouped_dataframe, start_hour='12:30', end_hour='14:00')
  else:
    print(selection)
  return ''

def get_messages(count, oldest):
  query_slack_client = SlackClient(OAUTH_ACCESS_TOKEN)
  print('Fetching a max of ' + str(count) + ' messages since ' + str(oldest))
  history = query_slack_client.api_call(
    'channels.history',
    channel='C37ELNXTK',
    oldest=oldest,
    count=count
  )
  if 'messages' not in history:
    print('Failed to read messages from channel')
    return []
  return history['messages']

if __name__ == '__main__':
  print('Starting app')
  port = int(os.environ.get('PORT', 5000))
  app.run(host='0.0.0.0', port=port, debug=True)
