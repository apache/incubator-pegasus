# Pegasus Docker

This project maintains the stuff you can use to build Pegasus docker images,
to deploy a standalone cluster of dockers on your local machine.

## Guide

It consists of the following components:

- [pegasus-docker-compose](/pegasus-docker-compose/README.md)

- [pegasus-build-env](/pegasus-build-env/README.md)

## Uploading an image to DockerHub 

If you are the contributor of this project, you may need to push the built images
to DockerHub. Firstly, you should be one of the members of the orgnization [apachepegasus](https://hub.docker.com/orgs/apachepegasus).

```sh
docker login --username=yourhubusername --email=youremail@company.com
```

Then push the image to DockerHub.
