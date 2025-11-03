pipeline {
  agent any
  stages {
    stage('Checkout') {
      steps {
        checkout scm
      }
    }

    stage('Lint') {
      steps {
        sh '''
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
            echo "$DOCKERHUB_TOKEN" | docker login -u "$DOCKERHUB_USERNAME" --password-stdin

            # Build & push to Docker Hub repository: topologist
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
