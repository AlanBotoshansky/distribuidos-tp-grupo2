import random

def fail_with_probability(probability, description=""):
    """
    Randomly fail with a given probability.
    
    Args:
        probability (float): Probability of failure (between 0 and 1).
        description (str): Optional description of the failure.
    
    Raises:
        RuntimeError: If the random failure occurs, a RuntimeError is raised with a message.
    """
    
    if random.random() < probability:
        msg = f"Simulated failure"
        if description:
            msg += f": {description}"
        raise RuntimeError(msg)
