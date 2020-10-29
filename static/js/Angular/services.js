angular.module('App.services', [])
  .factory('coursesService', function($http) {

    var service = {};

    service.getDrivers = function() {
      return $http({
        dataType: 'JSONP', 
        url: 'http://ergast.com/api/f1/2013/driverStandings.json'
      });
    }

    service.getDriverDetails = function(id) {
      return $http({
        dataType: 'JSONP',
        url: 'http://ergast.com/api/f1/2013/drivers/'+ id +'/driverStandings.json'
      });
    }

    service.getDriverRaces = function(id) {
      return $http({
        dataType: 'JSONP',
        url: 'http://ergast.com/api/f1/2013/drivers/'+ id +'/results.json'
      });
    }

    service.getHello = function() {
      return $http({
        dataType: 'JSONP',
        url: '/getPerson'
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

    return service;
  });
