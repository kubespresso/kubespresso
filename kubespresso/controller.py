#!/bin/env python3

def main():
    """The main loop of the controller. Watch for objects of type `Job` and
    inspect it's labels. If the object has the `expectedDuration` label set,
    pass it to the handler.
    """
    pass


def handle_jobs(job_object: dict):
    """Check if we need to act upon this specific object.
    """
    pass


def should_make_coffe(job_object: dict) -> bool:
    """Check if this is the first time we see this `Job` and if the owner didn't
    get it's coffee earlier today.
    """
    pass


def make_coffee():
    """Send an API call to the coffee machine to make a cup of coffee.
    """
    pass


if __name__ == '__main__':
    main()
