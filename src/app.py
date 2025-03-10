import os
from pathlib import Path

import dxdy.tui.dxdy_app as dxdy_app
from dxdy.settings import Settings


if __name__ == "__main__":

    #test_tmp_dir = Path('./tests/test_data')
    # copy the db file to the temporary directory
    #test_db_file = os.path.join(test_tmp_dir.name, 'test.db')

    
    app = dxdy_app.DxDyApp()
    app.run()
    
