angular.module('App.services', [])
  .factory('coursesService', function($http) {

    var service = {};

    service.getHello = function() {
      return $http({
        dataType: 'JSONP',
        url: '/getPerson'
      });
    }

    service.getTopics = function() {
      return $http({
        dataType: 'JSONP',
        url: '/getTopics'
      });
    }

    service.getCourses = function() {
      return $http({
        dataType: 'JSONP',
        url: '/getAllCourses'
      });
    }

    service.getCoursesDetails = function() {
      return $http({
        dataType: 'JSONP',
        url: '/getCoursesDetails'
      });
    }

    service.getCountries = function() {
      return $http({
        dataType: 'JSONP',
        url: '/getCountries'
      });
    }

    service.getCoursesByUser = function(userId) {
      return $http({
        dataType: 'JSONP',
        url: '/getCourses?userId='+userId
      });
    }

    service.getCoursesByTopic = function(topic) {
      return $http({
        dataType: 'JSONP',
        url: '/getCourses?topic='+topic
      });
    }

    service.validateGoogleUser = function(tokenId) {
      return $http({
        dataType: 'JSONP',
        url: '/googleAuth/'+tokenId
      });
    }

    service.validateRegularSignInUp = function(isSignIn, email, password,user_name){
      return $http({
        dataType: 'JSONP',
        url: '/validateUser/'+isSignIn +'/'+email+'/'+password+'/'+user_name
      });
      
     /*
      const promise1 = new Promise(function(resolve, reject) {
        var response = {'data': {user_name : userName, password : "blablas", 
        email:'dan_ko@outlook.com', response_message:""}}
        setTimeout(function() {
          resolve(response);
        }, 300);
      });
      return promise1;
      */
    }

    service.getUser = function(email){
      return $http({
        dataType: 'JSONP',
        url: '/getUser/'+email
      });
    }

    service.updateUser = function(user){
      return $http.post('/updateUser', JSON.stringify(user))
    }
    /*
    service.updateUserCourses = function(email,courses){
      return $http.post('/updateUserCourses', JSON.stringify({email:email,courses:courses}))
    }
    */
   service.addNewCourse = function(newCourse){
    return $http.post('/addNewCourse', JSON.stringify(newCourse))
  }
    
    return service;
  });
