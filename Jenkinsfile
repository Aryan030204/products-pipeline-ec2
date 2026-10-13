// CI/CD for products-pipeline-ec2.
//
//   Build image  ->  Compile checks  ->  Unit tests  ->  Deploy (auto-rollback)
//
// Any failure before Deploy stops the build with the running stack untouched.
// Deploy ships the exact image that passed the checks (no rebuild), and
// scripts/deploy.sh rolls back to the previous image automatically if the new
// containers don't come up healthy.
//
// Agent requirements: runs ON the EC2 host that serves the pipeline, with
// docker + docker compose v2 available to the Jenkins user (in the `docker`
// group).
//
// Jenkins credentials:
//   products-pipeline-env  (Secret file) — the production .env. Copied into the
//   workspace only for the deploy stage, because compose needs it for
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
    IMAGE = "products-pipeline:${env.BUILD_NUMBER}"
  }

  stages {
    stage('Build image') {
      steps {
        sh 'docker build --pull -t "$IMAGE" .'
      }
    }

    stage('Compile checks') {
      steps {
        sh '''
          docker run --rm "$IMAGE" sh -ec '
            for f in *.js test/*.js; do node --check "$f"; done
            sh -n backfill-cron-entrypoint.sh
            sh -n backfill-cron-run.sh
            npm ls --omit=dev >/dev/null
          '
          bash -n scripts/deploy.sh
        '''
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
            "$IMAGE" \
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
            install -m 600 "$PROD_ENV_FILE" .env
            bash scripts/deploy.sh "$IMAGE"
          '''
        }
      }
    }
  }

  post {
    always {
      // Drop the build tag: on success products-pipeline:latest still points at
      // the image; on failure it's the rejected image. :rollback is kept.
      sh '''
        rm -f .env
        docker rmi "$IMAGE" >/dev/null 2>&1 || true
        docker image prune -f >/dev/null 2>&1 || true
      '''
    }
  }
}
