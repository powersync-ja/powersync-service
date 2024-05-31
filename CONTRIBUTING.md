# Contributing to PowerSync Service

We want this community to be friendly and respectful to each other. Please follow it in all your interactions with the project.

All types of contributions are encouraged and valued. See the [Table of Contents](#table-of-contents) for different ways to help and details about how this project handles them. Please make sure to read the relevant section before making your contribution. It will make it a lot easier for us maintainers and smooth out the experience for all involved. The community looks forward to your contributions.

> And if you like the project, but just don't have time to contribute, that's fine. There are other easy ways to support the project and show your appreciation, which we would also be very happy about:
>
> - Star the project
> - Tweet about it
> - Refer this project in your project's readme
> - Mention the project at local meetups and tell your friends/colleagues

## Table of Contents

- [I Want To Contribute](#i-want-to-contribute)
  - [Reporting Bugs](#reporting-bugs)
  - [Suggesting Enhancements](#suggesting-enhancements)
  - [Code Formatting](#code-formatting)
  - [Pull Requests](#pull-requests)
- [Legal Notice](#legal-notice)
- [Attribution](#attribution)

## I Want To Contribute

### Reporting Bugs

#### Before Submitting a Bug Report

- Ensure you are using the latest version.
- Verify that the issue is not due to an error on your side (e.g., incompatible environment components/versions).
- Check the [documentation](https://docs.powersync.com/) and search the [issue tracker](https://github.com/powersync-ja/powersync-service/issues?q=label%3Abug) to see if the issue has already been reported or resolved.
- Collect relevant information:
  - Stack trace
  - OS, Platform, and Version
  - Interpreter, compiler, SDK, runtime environment, package manager versions
  - Steps to reproduce the issue

#### Submitting a Bug Report

> Do not report security issues, vulnerabilities, or bugs containing sensitive information publicly. Instead, email them to [support](mailto:support@powersync.com).

- Open a thread on [Discord](https://discord.gg/powersync) or [Issue](https://github.com/powersync-ja/powersync-service/issues/new).
- Describe the expected behavior and the actual behavior.
- Provide context and steps to reproduce the issue, including code if possible.
- Include the collected information.

### Suggesting Enhancements

#### Before Submitting an Enhancement

- Ensure you are using the latest version.
- Check the [documentation](https://docs.powersync.com/) to see if the functionality already exists.
- Search the [Discord](https://discord.gg/powersync) and [issue tracker](https://github.com/powersync-ja/powersync-service/issues) to see if the enhancement has already been suggested. If so, comment on the existing issue.
- Ensure your idea fits the project's scope and goals.

#### Submitting an Enhancement Suggestion

- Use a clear and descriptive title for the issue.
- Provide a detailed description of the suggested enhancement.
- Describe the current behavior and explain the expected behavior.
- Include screenshots or GIFs if applicable.
- Explain why the enhancement would be useful to most users.

### Code Formatting

The project uses [Prettier](https://prettier.io/) for formatting. If your editor isn't set up to work with it, you can format all files from the command line with

```bash
pnpm format
```

### Pull Requests

> **Working on your first pull request?** You can learn how from this _free_ series: [How to Contribute to an Open Source Project on GitHub](https://egghead.io/courses/how-to-contribute-to-an-open-source-project-on-github).

When you're submitting a pull request:

- Prefer small pull requests focused on one change.
- Verify that linters and tests are passing.
- Review the code/README documentation to make sure it looks good.
- Follow the pull request template when opening a pull request.
- For pull requests that change the API or implementation, discuss with maintainers first by opening an issue.

Pull requests should contain Changesets for changed packages.

Add changesets with

```Bash
pnpm changeset add
```

Merging a PR with Changesets will automatically create a PR with version bumps. That PR will be merged when releasing.

## Legal Notice

This repo requires acceptance of our [CLA](https://www.powersync.com/legal/cla) in order to contribute. We use [cla-assistant](https://cla-assistant.io/) to automate this process.

## Attribution

This guide is based on the **contributing-gen**. [Make your own](https://github.com/bttger/contributing-gen)!
