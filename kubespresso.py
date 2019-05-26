import logging
import os

from kubernetes import client, config, watch


def cluster_login():
    if 'KUBERNETES_PORT' in os.environ:
        logging.debug('Running inside of a cluster')
        config.load_incluster_config()
    else:
        logging.debug('Running outside of a cluster')
        config.load_kube_config()


def setup_logger(level=logging.INFO):
    logging.basicConfig()
    logging.getLogger().setLevel(level)


def process_stream(stream, handlers):
    logging.info('Start processing event stream')
    for event in stream:
        # We use raw_object in order to make the example simpler]
        for h in handlers:
            h(event)


def logger_handler(event):
    # event can be: ADDED, MODIFIED, DELETED
    obj_name = event['raw_object']['metadata']['name']
    event_type = event['type']
    logging.info(f'handling event {event_type} for {obj_name}')


def main():
    setup_logger(logging.DEBUG)
    cluster_login()
    batch_api = client.BatchV1Api()
    stream = watch.Watch().stream(batch_api.list_job_for_all_namespaces)
    while True:
        process_stream(stream, [logger_handler])


if __name__ == '__main__':
    main()
