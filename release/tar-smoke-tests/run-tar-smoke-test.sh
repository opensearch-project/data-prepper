#
# /*
#  * Copyright OpenSearch Contributors
#  * SPDX-License-Identifier: Apache-2.0
#  */
#

docker build -t data-prepper-tar:latest .
docker run --rm data-prepper-tar:latest
