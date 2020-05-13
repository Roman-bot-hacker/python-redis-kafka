from controllers.AppController import RedisController
from initials import api, app, config
import logging

logging.basicConfig(level=logging.INFO)


@app.route('/')
def hello_world():
    return 'Hello World!'


api.add_resource(RedisController, "/lab5")


if __name__ == '__main__':

    try:
        writer_mode = config['writer']
    except KeyError:
        logging.error("Writer mode not found in config")
        raise

    app.run(debug=True)
