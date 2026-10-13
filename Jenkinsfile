// CI/CD for products-pipeline-ec2.
//
// 1. Build the Docker image from the checked-out commit.
// 2. Run the unit tests inside that image (clean env, no DB/network needed).
// 3. Only if every test passes: `docker compose down` the running stack and
//    bring it back up from this commit.
//
// Agent requirements: runs ON the EC2 host that serves the pipeline, with
// docker + docker compose v2 available to the Jenkins user (in the `docker`
// group).
//
// Jenkins credentials:
//   products-pipeline-env  (Secret file) — the production .env. It is copied
//   into the workspace only for the deploy stage, because compose needs it for
//   `env_file: .env` and to interpolate the backfill service's
//   ${GET_BRANDS_API} / ${PIPELINE_AUTH_HEADER} / ${PASSWORD_AES_KEY}.

pipeline {
  agent any

  options {
    disableConcurrentBuilds()
    timeout(time: 30, unit: 'MINUTES')
    buildDiscarder(logRotator(numToKeepStr: '20'))
    timestamps()
  }

  environment {
    // Pin the compose project name so `down` targets the stack that is already
    // running, regardless of the Jenkins workspace directory name.
    COMPOSE_PROJECT_NAME = 'products-pipeline-ec2'
    TEST_IMAGE = "products-pipeline-test:${env.BUILD_NUMBER}"
  }

  stages {
    stage('Build test image') {
      steps {
        sh 'docker build --pull -t "$TEST_IMAGE" .'
      }
    }

    stage('Unit tests') {
      steps {
        sh '''
          rm -rf test-results && mkdir -p test-results
          # No env_file / -e here on purpose: tests must not see production
          # config (e.g. BACKFILL_BRAND_INDICES in .env changes test behaviour).
          docker run --rm \
            --user "$(id -u):$(id -g)" \
            -v "$WORKSPACE/test-results:/results" \
            "$TEST_IMAGE" \
            sh -c 'node --test \
              --test-reporter=spec --test-reporter-destination=stdout \
              --test-reporter=junit --test-reporter-destination=/results/junit.xml \
              test/*.test.js'
        '''
      }
      post {
        always {
          junit allowEmptyResults: false, testResults: 'test-results/junit.xml'
        }
      }
    }

    stage('Deploy') {
      steps {
        withCredentials([file(credentialsId: 'products-pipeline-env', variable: 'PROD_ENV_FILE')]) {
          sh '''
            set -e
            install -m 600 "$PROD_ENV_FILE" .env

            # compose declares pipeline-net as external; make sure it exists.
            docker network inspect pipeline-net >/dev/null 2>&1 || docker network create pipeline-net

            # Build before stopping anything so a build failure leaves the old
            # stack running. Layers are cached from the test image build.
            docker compose build

            docker compose down --remove-orphans
            # Containers use fixed container_name values; clear any left behind
            # by a stack started under a different project name.
            docker rm -f products-pipeline-ec2 products-pipeline-backfill >/dev/null 2>&1 || true

            docker compose up -d
          '''
        }
      }
    }

    stage('Verify deploy') {
      steps {
        sh '''
          # restart: always hides crash loops, so wait, then require both
          # containers to be running and not restarting.
          sleep 20
          failed=0
          for c in products-pipeline-ec2 products-pipeline-backfill; do
            state=$(docker inspect -f '{{.State.Status}} restarts={{.RestartCount}}' "$c" 2>/dev/null || echo missing)
            echo "$c: $state"
            case "$state" in
              "running restarts=0") ;;
              *) failed=1; docker logs --tail 100 "$c" 2>&1 || true ;;
            esac
          done
          exit $failed
        '''
      }
    }
  }

  post {
    always {
      sh '''
        rm -f .env
        docker rmi "$TEST_IMAGE" >/dev/null 2>&1 || true
        docker image prune -f >/dev/null 2>&1 || true
      '''
    }
  }
}
