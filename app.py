from flask import Flask, render_template, request, redirect, url_for
from slackclient import SlackClient
from logic import create_dataframe, process_dataframe, get_restaurants_by_average_time, get_n_most_popular_restaurants, \
  get_n_earliest_restaurants, get_n_latest_restaurants, get_average_time_for_restaurant

import os
import json

app = Flask(__name__)

VERIFICATION_TOKEN = os.environ.get('VERIFICATION_TOKEN')
ACCESS_TOKEN = os.environ.get('ACCESS_TOKEN')

def get_messages(slack_client, count=1000):
  history = slack_client.api_call(
    'channels.history',
    channel='C37ELNXTK',
    count=count
  )
  return history['messages']

slack_client = SlackClient(ACCESS_TOKEN)

print('Getting messages from food channel')

messages = get_messages(slack_client=slack_client)

print('Creating dataframe')

dataframe = create_dataframe(messages=messages)

print('Processing dataframe')

grouped_dataframe = process_dataframe(dataframe=dataframe)

print('Data ready')

@app.route('/slack_event', methods=['POST'])
def slack_event():
  print('Get return message')
  req = json.loads(request.data)
  print('Parse request')
  if req.get('token') == VERIFICATION_TOKEN:
    print('Token verified')
    if 'challenge' in req:
      return req.get('challenge')
    event = req.get('event')
    if not event.get('bot_id') and 'text' in event:
      print('Generate buttons')
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
            {
              "name": "foodbot",
              "text": "After 12:30",
              "type": "button",
              "value": "second_half"
            }
          ]
        }
      ]
      print('Update message channel ' + event.get('channel'))
      slack_client.api_call(
        'chat.postMessage',
        channel=event.get('channel'),
        text='',
        attachments=json.dumps(attachments)
      )
      print('Channel updated')
    return ''

@app.route('/slack_action', methods=['POST'])
def slack_action():
  form_json = json.loads(request.form['payload'])
  if form_json.get('token') == VERIFICATION_TOKEN:
    selection = form_json.get('actions')[0].get('value')
    slack_client.api_call(
      'chat.update',
      channel=form_json.get('channel').get('id'),
      ts=form_json.get('message_ts'),
      text=get_results_from_selection(selection=selection),
      attachments=[]
    )
  return ''

@app.route('/debug', methods=['GET'])
def debug():
  return str(grouped_dataframe)

def get_results_from_selection(selection):
  if selection == 'popular':
    return get_n_most_popular_restaurants(dataframe=grouped_dataframe)
  elif selection == 'earliest':
    return get_n_earliest_restaurants(dataframe=grouped_dataframe)
  elif selection == 'latest':
    return get_n_latest_restaurants(dataframe=grouped_dataframe)
  elif selection == 'first_half':
    return get_restaurants_by_average_time(dataframe=grouped_dataframe, start_hour='11:00', end_hour='12:30')
  elif selection == 'second_half':
    return get_restaurants_by_average_time(dataframe=grouped_dataframe, start_hour='12:30', end_hour='14:00')
  return ''

if __name__ == '__main__':
  print('Starting app')
  port = int(os.environ.get('PORT', 5000))
  app.run(host='0.0.0.0', port=port, debug=True)