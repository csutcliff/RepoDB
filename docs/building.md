# Building the Solution

In this page, we will guide you on how to build the RepoDB Solution.

## Install Git

To install [Git](https://git-scm.com/), please follow this [guide](https://git-scm.com/book/en/v2/Getting-Started-Installing-Git).

## Clone the Repository

```
> C:
> mkdir c:\src
> cd c:\src
> git clone https://github.com/AmpScm/RepoDb.git
```

## Building RepoDb

```
> cd RepoDb
> dotnet build -v n
```

#### Executing the IntegrationTests.

For this we now assume that you have docker installed

```
> docker compose up -d
```

This will start docker services for sqlserver, mysql, oracle and postgresql. The first time this may be quite a heavy download,
but the storage is cached locally.

The code also allows attaching to existing services using environment strings, but if possible I would recommend using the docker
environment as that allows clean tests. We start the same environment on GitHub for continuous integration.

In `/older/` is an alternate docker configuration with older versions of the all the dependencies, to improve test coverage
against older engines. You should start the settings in root, or those in older... not both at once.


Once you have the environment started running the tests is as easy as

```
> dotnet test --no-build
```

You can also run the tests from Visual Studio and or Visual Studio Code using their testrunners. These UIs also make it
easier to just run specific tests
