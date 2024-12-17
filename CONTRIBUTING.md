# Contributing

We welcome your input! We want to make contributing to this project as easy and transparent as possible. You can contribute by:

- Reporting bugs
- Discussing the current state of the code
- Submitting fixes
- Proposing new features
- Becoming a maintainer

## Development Workflow
We use [Github Flow](https://docs.github.com/en/get-started/using-github/github-flow) for collaboration. All changes happen through pull requests (PRs). In the early stages, changes are driven directly by us, but changes should typically be reflected through PRs.

1. Fork this repository and clone your fork locally.
2. Create a new branch from **master**:
```bash
git checkout -b feature/my-new-feature-or-issue-id
```
3. Make your changes:
- Add tests for any new functionality.
- Update documentation if APIs or behaviors change.
4. Ensure the following before submitting:
- Your code passes the test suite.
- Your code adheres to the project's coding style.
5. Open a pull request (PR) and describe your changes clearly.

We actively welcome and review pull requests, so don’t hesitate to contribute!


## Reporting Bugs
We use GitHub Issues to track bugs and feature requests. To report a bug:

1. Search existing issues first to avoid duplicates.
2. If no similar issue exists, open a new issue with:
    - A clear and descriptive title.
    - Steps to reproduce the issue:
        - Include sample code if possible.
        - Be specific (e.g., system version, inputs, outputs).
    - Expected behavior vs. actual behavior.
    - Additional context, logs, or screenshots to help us diagnose the issue.

Please use the templates provided in the **.github** directory.
Thorough bug reports help us fix issues faster. Thank you!

## Submitting Changes via Pull Requests
Follow these steps to submit changes:

1. Fork the repository and clone it locally.
2. Create a branch for your changes:
```bash
git checkout -b feature/my-new-feature-or-issue-id
```
3. Make your changes:
- Add tests for new code.
- Update documentation if necessary.
4. Ensure your changes pass the test suite:
```bash
go test ./...
```
5. Check your code formatting:
```bash
go fmt ./...
```
6. Commit your changes with a descriptive message:
```bash
git commit -m "Add: Description of the feature or fix"
```
7. Push to your fork:
```bash
git push origin feature/my-new-feature-or-issue-id
```
8. Open a Pull Request targeting the **master** branch.

Please use the templates provided in the **.github** directory.

## License
By contributing, you agree that your submissions are licensed under the [MIT License](http://choosealicense.com/licenses/mit/).
