# module to display and render to the console
import datetime as dt
def print_title(title_text:str):
    print(' '.join(['***', title_text, '***']))


def print_event(event_text:str):
    print(', '.join([str(dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S')), str(event_text)]))