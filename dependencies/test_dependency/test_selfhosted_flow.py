
from prefect import flow, task

@task
def hello_task():
    return "Hello from Prefect self-hosted!"

@flow
def test_flow():
    message = hello_task()
    print(message)
    return message

if __name__ == "__main__":
    test_flow()
