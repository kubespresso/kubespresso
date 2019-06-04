import logging
import os
import time
from textwrap import dedent

from kubernetes import client, config, watch


LAST_MODIFIED_LABEL = 'kubespresso.io/lastCoffeeGranted'
EXPECTED_DURATION_LABEL = 'kubespresso.io/expectedDuration'


class Color(object):
    DEFAULT = '\x1b[0m'
    RED = '\x1b[31m'
    GREEN = '\x1b[32m'
    YELLOW = '\x1b[33m'
    CYAN = '\x1b[36m'

    @classmethod
    def colored(cls, color, message):
        """Small function to wrap a string around a color"""
        return getattr(cls, color.upper()) + message + cls.DEFAULT


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
    logging.basicConfig(format='* %(message)s')
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
    ok, reason = event_deserves_coffee(event)
    if not ok:
        logging.info(Color.colored('yellow', f'Sorry, {reason}'))
        return
    ok, reason = annotate_pod(event)
    if not ok:
        logging.info(Color.colored('yellow', f'Sorry, {reason}'))
        return
    logging.info(Color.colored('green', f'Good news! you\'re getting a coffee'))
    make_coffee()


def annotate_pod(event) -> (bool, str):
    """Annotate the pod under the event

    :rtype: tuple(bool, str)
    :returns: A tuple that indicates if the pod was annotated succesfully or not
             and a string representation of the outcome.
    """
    obj = event['object']
    patch = generate_annotation_patch(event)
    obj_meta = obj.metadata
    patch_applied = apply_patch_on_pod(obj_meta.name, obj_meta.namespace, patch)
    if patch_applied:
        return True, 'You definitely deserve a coffee!'
    return False, 'There is a newer version of the object. Please wait'


def event_deserves_coffee(event):
    obj = event['object']
    if event['type'] not in ('ADDED', 'MODIFIED'):
            return False, 'coffee is granted only on ADDED or MODIFIED Pods'
    if obj.kind != 'Pod':
        return False, 'coffee is granted only for Pod objects'
    expected_duration = int(
        extract_field_from_event_annotations(event, EXPECTED_DURATION_LABEL)
        or 0
    )
    if expected_duration < 60:
        return False, "Pod's expected duration is less than 60"
    if seconds_since_last_modification(event) < 86400:  # 1 day in seconds
        return False, 'you already received a coffee today'
    return True, 'you deserve a coffee'


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
    logging.info(Color.colored('cyan', 'Hold on... I\'m boiling the water...'))
    time.sleep(3)
    logging.info(Color.colored('cyan', 'Adding sugar...'))
    time.sleep(2)
    logging.info(Color.colored('cyan', 'Voila!'))
    time.sleep(0.2)
    logging.info(Color.colored(
        'cyan',
        """
               {
            {   }
            }_{ __{
            .-{   }   }-.
        (   }     {   )
        |`-.._____..-'|
        |             ;--.
        |            (__  \\
        |             | )  )
        |             |/  /
        |             /  /
        |            (  /
        \             y'
         `-.._____..-'
        """
    ))


def generate_annotation_patch(event) -> dict:
    """Generate `LAST_MODIFIED_LABEL` annotation patch

    In order to keep track of events we already processed, we set an
    annotation with a timestamp of the last time we processed this object. This
    method generates a patch object that can be applied via the API.

    :param Event event: Object representing an event. This object is generated
                        by the `unmarshal_event` method of the `Watch` object.
                        It's defined under `kubernetes.base.watch`.
    :rtype: dict
    :returns: dict with a patch to be applied on the pod.
    """
    event_object = event['object']
    resource_version = event_object.metadata.resource_version or '0'
    now = str(int(time.time()))
    object_meta = client.V1ObjectMeta(
        annotations={LAST_MODIFIED_LABEL: now},
        resource_version=resource_version
    )
    # obj_name = event_object.metadata.name
    # ns = event_object.metadata.namespace
    return {'metadata': object_meta}


def apply_patch_on_pod(object_name:str , namespace: str, patch: object) -> bool:
    """Safely patch the given `object_name` with the provided object (`body`)

    :rtype: bool
    :returns: True if the patch was applied successfully and False in case of
              a conflict.
    """
    core_v1_api = client.CoreV1Api()
    try:
        core_v1_api.patch_namespaced_pod(object_name, namespace, patch)
        logging.info(Color.colored(
            'green', f'Patched {object_name}')
        )
        return True
    except client.rest.ApiException as api_exception:
        if api_exception.reason == 'Conflict':
            logging.info(Color.colored(
                'red', 'There is a newer version of the object')
            )
            return False
        raise


def extract_field_from_event_annotations(event, annotation) -> str:
    """Given an event on `Pod` object, return the requested annotation

    We need this method because on some Pods, `metadata.annotations` might not
    be set and the default value for annotation is None.

    :rtype: str
    :returns: The requested annotation or an empty string if it doesn't exist.
    """
    annotations = event['object'].metadata.annotations
    if annotations:
        return annotations.get(annotation, '')
    return ''


def main():
    setup_logger(logging.INFO)
    cluster_login()
    core_v1_api = client.CoreV1Api()
    stream = watch.Watch().stream(core_v1_api.list_pod_for_all_namespaces)
    event_handlers = [logger_handler, coffee_handler]
    while True:  # In case a timeout will occur during stream processing
        process_stream(stream, event_handlers)


if __name__ == '__main__':
    main()
