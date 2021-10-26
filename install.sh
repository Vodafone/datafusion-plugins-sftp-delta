#!/bin/bash

NAMESPACE="default"
ENDPOINT="http://localhost:11015"

ID=$(cat pom.xml | python3 -c "import sys; import simplexml; xml=sys.stdin.read(); data = simplexml.loads(xml); print(data['project']['artifactId'])")
VERSION=$(cat pom.xml | python3 -c "import sys; import simplexml; xml=sys.stdin.read(); data = simplexml.loads(xml); print(data['project']['version'])")

JAR="$(pwd)/target/${ID}-${VERSION}.jar"
JSON="$(pwd)/target/${ID}-${VERSION}.json"

DEL_DEPLOY="/v3/namespaces/${NAMESPACE//\"/}/artifacts/$ID/versions/$VERSION"
JAR_DEPLOY="/v3/namespaces/${NAMESPACE//\"/}/artifacts/$ID"
JSON_DEPLOY="/v3/namespaces/${NAMESPACE//\"/}/artifacts/$ID/versions/$VERSION/properties"
PARENTS=$(jq '.parents[]' "$JSON" | sed 's/"//g' | tr '\n' '/' | sed 's/.$//')

echo "DELETE PREVIOUS..."
curl -X DELETE "${ENDPOINT}${DEL_DEPLOY}" \
    -H "Artifact-Extends: $PARENTS" \
    -H "Artifact-Version: $VERSION"
echo

echo "JAR..."
curl -X POST "${ENDPOINT}${JAR_DEPLOY}" \
    -H "Artifact-Extends: $PARENTS" \
    -H "Artifact-Version: $VERSION" \
    --data-binary @"$JAR"
echo

json_data=$(jq '.properties' "$JSON")
echo "JSON..."
curl -w"\n" -X PUT "${ENDPOINT}${JSON_DEPLOY}" \
    -H "Content-Type: application/json" \
    -d "$json_data"
echo
