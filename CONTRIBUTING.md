# Contributing

Thank you for wanting to contribute to Farsante! Here is a guide to get started on your first PR.

If this is your first time working with git, here is a helpful [GitHub resource](https://docs.github.com/en/pull-requests/collaborating-with-pull-requests/working-with-forks/fork-a-repo) for some of the terminology below like `fork`, `clone`, and `branch`

---

## Workflow

### 1. Fork Farsante

### 2. Clone the forked repository to your local environment

### 3. Create and checkout a new feature branch

in this example, we will be working in `a-new-feature`

```bash
cd path/to/your/cloned/repo
git checkout -b a-new-feature
```

### 4. Install project dependencies

Farsante uses python and rust. Below details the current setup needed to work on this repository locally.

Here are links to download python and rust if you don't have them installed on your system:

- install [python](https://www.python.org/downloads/)
- install [rust](https://www.rust-lang.org/tools/install)

#### a. Python

- Use [poetry](https://python-poetry.org/) to create a virtualenv using a version of python on your system. In this example, we are using [pyenv](https://github.com/pyenv/pyenv) to make python version selection easier.

set up environment for the virtualenv install

```bash
pyenv install 3.10.9
pyenv shell 3.10.9
pip install poetry
```

install required dependencies

```bash
poetry install
poetry install
```

install optional dev dependencies

```bash
poetry install --with dev
```

Now that you have a virtualenv installed with all of the required dependencies, you can run scripts (like unit tests)

using a poetry shell

```bash
poetry shell
pytest tests/
```

or with poetry run

```bash
poetry run pytest tests/
```

#### b. Rust

Farsante uses rust to generate large datasets quickly. You will need to use `maturin` to create a rust library that can be used by other python functions.

```bash
cd h2o-data-rust
maturin develop
```

### 5. Work on your changes in `a-new-feature`

Commit early and often. Working in small chunks makes reviews easier on other collaborators. Ensure that you don't break existing functionality by running unit tests, and add coverage for any new features with a unit test. Once you feel like your changes are ready to be reviewed, navigate to the `a-new-feature` branch on your GitHub fork and click on "make pull request".

Add additional context detailing what was changed, and reference any issues that this pull request addresses, like "fixes #123".

Other collaborators will review your PR, and those with merge permissions will merge your work into the main branch once no changes need to be made.

After your PR is merged, you can delete the `a-new-feature` branch from your fork.

---

## Collaboration

We discuss work on all MrPowers projects in the #mrpowers channel in this [slack workspace](https://join.slack.com/t/dataengineerthings/shared_invite/zt-2d77de29g-tyKACVZJmEIJNK0afbD3Pg). It's a good way to meet other data engineers and hone your programming skills!
