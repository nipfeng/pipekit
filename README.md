# pipekit

A lightweight Python library for composing and monitoring ETL pipelines with minimal boilerplate.

---

## Installation

```bash
pip install pipekit
```

---

## Usage

```python
from pipekit import Pipeline, step

@step
def extract(context):
    return {"data": [1, 2, 3, 4, 5]}

@step
def transform(context):
    return {"data": [x * 2 for x in context["data"]]}

@step
def load(context):
    print("Loading data:", context["data"])

pipeline = Pipeline(name="my_etl") \
    .pipe(extract) \
    .pipe(transform) \
    .pipe(load)

pipeline.run()
```

pipekit automatically tracks step execution time, logs progress, and surfaces errors with full context — no extra configuration needed.

---

## Features

- **Composable steps** — chain functions into clean, readable pipelines
- **Built-in monitoring** — execution time and status logged per step
- **Minimal setup** — no config files, no external dependencies
- **Error handling** — failures include step name and input context

---

## License

This project is licensed under the [MIT License](LICENSE).

---

## Contributing

Pull requests are welcome. For major changes, please open an issue first to discuss what you would like to change.