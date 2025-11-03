pipeline {
  agent any

  stages {
    stage('Checkout') {
      steps { checkout scm }
    }

    stage('Lint') {
      steps {
        sh '''
          set -e
          python3 -m venv .venv
          . .venv/bin/activate
          pip install --upgrade pip
          pip install flake8 black
          flake8 .
          black --check .
        '''
      }
    }

    stage('Docker Build & Push') {
      steps {
        withCredentials([usernamePassword(credentialsId: 'dockerhub-creds',
          usernameVariable: 'DOCKERHUB_USERNAME', passwordVariable: 'DOCKERHUB_TOKEN')]) {
          sh '''
            set -e
            docker --version || true
            echo "$DOCKERHUB_TOKEN" | docker login -u "$DOCKERHUB_USERNAME" --password-stdin

            docker build -t $DOCKERHUB_USERNAME/topologist:web ./web
            docker build -t $DOCKERHUB_USERNAME/topologist:worker ./worker
            docker build -t $DOCKERHUB_USERNAME/topologist:scheduler ./scheduler

            docker push $DOCKERHUB_USERNAME/topologist:web
            docker push $DOCKERHUB_USERNAME/topologist:worker
            docker push $DOCKERHUB_USERNAME/topologist:scheduler
          '''
        }
      }
    }
  }
}
