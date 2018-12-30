#!/usr/bin/env bash

# Release process:
#
# 1. Making a release requires PGP signing. Please make sure you generated a key.
#   See https://www.scala-sbt.org/sbt-pgp/usage.html
#
# 2. Make sure your key is uploaded to one of keyservers. For instance, it is required to
#   run 'sbt "pgp-cmd receive-key <key id> hkp://keyserver.ubuntu.com"'.
#   See https://www.scala-sbt.org/sbt-pgp/usage.html
#
# 3. You should have an access to Databricks domain. This is typically
#   requested by JIRA in Sonatype per individual.
#   See https://issues.sonatype.org/browse/OSSRH-19509
#
# 4. Run this script, for instance, as below:
#   'USERNAME=username PASSWORD=password dev/make-release.sh'
#   USERNAME and PASSWORD are the user name and password for Sonatype Nexsus Repository Manager.
#
# 5. After pushing the artifect to staging repogitory by running this script, you can manually
#   release it in https://oss.sonatype.org.
#   See also https://www.scala-sbt.org/1.x/docs/Using-Sonatype.html

if [[ -z "${USERNAME}" ]]; then
  echo "'USERNAME' environment varible for 'Sonatype Nexus Repository Manager' is undefiend."
  exit 1
fi

if [[ -z "${PASSWORD}" ]]; then
  echo "'PASSWORD' environment varible for 'Sonatype Nexus Repository Manager' is undefiend."
  exit 1
fi

sbt clean "+ publishSigned"
