import logging
import os
import time

from kubernetes import client, config, watch


LAST_MODIFIED_LABEL = 'kubespresso.io/lastCoffeeGranted'
EXPECTED_DURATION_LABEL = 'kubespresso.io/expectedDuration'


def cluster_login():
    """Initialize kubeconfig so we can communicate with the API server

    Depands on where we're running from (local or from within the cluster),
    we need a different API call to setup kubeconfig.
    """
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
        for handler in handlers:
            handler(event)


def logger_handler(event):
    """Show all the events we see during the processing loop

    This handler exists for debugging. It just prints every event it encounters
    to the log so that the controller becomes more verbose.

    :param Event event: Object representing an event. This object is generated
                        by the `unmarshal_event` method of the `Watch` object.
                        It's defined under `kubernetes.base.watch`.
    """
    # event can be: ADDED, MODIFIED, DELETED
    obj_name = event['raw_object']['metadata']['name']
    event_type = event['type']
    metadata = event['raw_object']['metadata']
    logging.info(f'handling event {event_type} for {obj_name}')
    logging.debug(f'metadata for {obj_name}: {metadata}')


def coffee_handler(event):
    """Process an event and make coffee if needed

    :param Event event: Object representing an event. This object is generated
                    by the `unmarshal_event` method of the `Watch` object.
                    It's defined under `kubernetes.base.watch`.
    """
    deserves_coffee, explain = should_perform_on(event)
    if not deserves_coffee:
        logging.info(f'Sorry, {explain}')
        return
    logging.info(f'Good news! {explain}')
    make_coffee()
    touch_job_event(event)



def should_perform_on(event) -> (bool, str):
    """Given an event, decide if we should perform on it or not

    This controller performs on ADDED or MODIFIED event types and only on Job
    objects.

    :param Event event: Object representing an event. This object is generated
                        by the `unmarshal_event` method of the `Watch` object.
                        It's defined under `kubernetes.base.watch`.

    :rtype: tuple(bool, str)
    :returns: A tuple where the first element is True if we perform on this kind
              of event and False otherwise. The second element is a string with
              the reason for the decision.
    """
    if event['type'] not in ('ADDED', 'MODIFIED'):
        return False, 'coffee is granted only on ADDED or MODIFIED Jobs'
    if event['raw_object']['kind'] != 'Job':
        return False, 'coffee is granted only for Job objects'
    job_expected_duration = int(
        extract_field_from_event_annotations(event, EXPECTED_DURATION_LABEL)
        or 0
    )
    if job_expected_duration < 60:
        return False, "Job's expected duration is less than 60"
    if seconds_since_last_modification(event) < 86400:  # 1 day in seconds
        return False, 'you already received a coffee today'
    return True, 'you definitely deserve a coffee!'


def extract_field_from_event_annotations(event, annotation) -> str:
    """Given a Job event event, return the requested annotation

    We need this method because on some Jobs, `metadata.annotations` might not
    be set and the default value for annotation is None.

    :rtype: str
    :returns: The requested annotation or an empty string if it doesn't exist.
    """
    annotations = event['object'].metadata.annotations
    if annotations:
        return annotations.get(annotation, '')
    return ''


def seconds_since_last_modification(event) -> int:
    """Given an event, return the time passed since the last time we touched it

    :param Event event: Object representing an event. This object is generated
                        by the `unmarshal_event` method of the `Watch` object.
                        It's defined under `kubernetes.base.watch`.

    :rtype: int
    :returns: Seconds since the last time we touched this event
    """
    last_modified = int(
        extract_field_from_event_annotations(event, LAST_MODIFIED_LABEL) or 0
    )
    return int(time.time()) - last_modified


def make_coffee():
    """Do an API call to the coffee machine and make a coffee
    """
    # ... some coffe machine specific code goes here ....
    logging.info('Coffe is waiting for you in the kitchen!')


def touch_job_event(event):
    """Set a `last_modified` annotation on a given event

    In order to keep track of events we already processed, we can set an
    annotation with a timestamp of the last time we processed this object.

    :param Event event: Object representing an event. This object is generated
                        by the `unmarshal_event` method of the `Watch` object.
                        It's defined under `kubernetes.base.watch`.
    """
    v1_job = event['object']
    if not v1_job.metadata.annotations:
            v1_job.metadata.annotations = {}
    v1_job.metadata.annotations.update({
        LAST_MODIFIED_LABEL: str(int(time.time()))
    })
    obj_name = v1_job.metadata.name
    ns = v1_job.metadata.namespace
    batch_api = client.BatchV1Api()
    batch_api.patch_namespaced_job(obj_name, ns, {'metadata': v1_job.metadata})
    logging.debug(f'Set {LAST_MODIFIED_LABEL} on {obj_name}')


def main():
    setup_logger(logging.INFO)
    cluster_login()
    batch_api = client.BatchV1Api()
    stream = watch.Watch().stream(batch_api.list_job_for_all_namespaces)
    event_handlers = [logger_handler, coffee_handler]
    while True:  # In case a timeout will occur during stream processing
        process_stream(stream, event_handlers)


if __name__ == '__main__':
    main()
