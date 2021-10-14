FROM gitpod/workspace-full

SHELL ["/bin/bash", "-c"]
USER gitpod

RUN curl -s "https://get.sdkman.io" | bash
RUN . /home/gitpod/.sdkman/bin/sdkman-init.sh && sdk selfupdate force
RUN . /home/gitpod/.sdkman/bin/sdkman-init.sh && sdk install java $(sdk list java | grep -o "8\.[0-9]*\.[0-9]*\.hs-adpt" | head -1)
RUN . /home/gitpod/.sdkman/bin/sdkman-init.sh && sdk install sbt