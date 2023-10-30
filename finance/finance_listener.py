from watchdog.events import FileSystemEventHandler
from finance.finance_extractor import FileStager
from finance.finance_extractor import load
from finance.finance_extractor import convert
from pathlib import Path
import finance.output as o

class ComptesModifiedHandler(FileSystemEventHandler):
    def on_modified(self, event):
        if not event.is_directory:
            filename = event.src_path
            fs = FileStager()
            p = Path(filename)
            if fs.is_valid_file(p.name):
                o.print_event(f'Fichier Modifié : {p.name}')
                o.print_title('starting conversion')
                convert()
                print('starting loading')
                load()
            else:
                o.print_event(f'Invalid File : {event.src_path}')
