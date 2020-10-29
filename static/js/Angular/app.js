angular.module('App', [
  'App.services',
  'App.controllers',
  'ngRoute',
  'myApp.directives'
]).
config( function($routeProvider,$locationProvider) {
    $locationProvider.hashPrefix('');
    $routeProvider.
        when("/drivers", {templateUrl: "partials/drivers.html", controller: "driversController"}).
        when("/drivers/:id", {templateUrl: "partials/driver.html", controller: "driverController"}).
        when("/home",{templateUrl: "partials/home.html", controller: "homeController"}).
        when("/register",{templateUrl: "partials/log.html", controller: "registerController"})
        
        .otherwise({redirectTo: '/home'});
});
