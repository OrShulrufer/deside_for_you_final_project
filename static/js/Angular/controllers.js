angular.module('App.controllers', []).

  /* Drivers controller */
  controller('driversController', function($scope, coursesService) {
    $scope.nameFilter = null;
    $scope.driversList = [];
    $scope.searchFilter = function (driver) {
        var re = new RegExp($scope.nameFilter, 'i');
        return !$scope.nameFilter || re.test(driver.Driver.givenName) || re.test(driver.Driver.familyName);
    };

    coursesService.getDrivers().then(function (response) {
        //Digging into the response to get the relevant data
        $scope.driversList = response.data.MRData.StandingsTable.StandingsLists[0].DriverStandings;
    });
  }).

  /* Driver controller */
  controller('driverController', function($scope, $routeParams, coursesService) {
    $scope.id = $routeParams.id;
    $scope.races = [];
    $scope.driver = null;

    coursesService.getDriverDetails($scope.id).then(function (response) {
        $scope.driver = response.data.MRData.StandingsTable.StandingsLists[0].DriverStandings[0];
    });

    coursesService.getDriverRaces($scope.id).then(function (response) {
        $scope.races = response.data.MRData.RaceTable.Races;
    }); 
  }).

  /* initall controller  */
  controller('userController', ['$rootScope','$routeParams','coursesService', function ($scope,$routeParams, coursesService) {
    //$('#cover-spin').show(0); //$('#cover-spin').hide();
    function googleSignIn (someParam) {
        $scope.googleAuthInstance = gapi.auth2.getAuthInstance()
        if($scope.googleAuthInstance.isSignedIn.G3.value) {
            //googleUser = "Dan Kozak";
            ShowHideGoogleSignIn(false);
            $scope.googleSignedIn=true;
            $scope.userName=$scope.googleAuthInstance.currentUser.get().getBasicProfile().Ad;
            //$scope.googleUser.uc.id_token
            //$scope.googleUser.uc.expires_in
            if(!$scope.$$phase) {
                $scope.$digest();
              }
            $('#cover-spin').hide();
            var id_token = $scope.googleAuthInstance.currentUser.get().getAuthResponse().id_token;
            coursesService.validateGoogleUser(id_token).then(function (response) {
                
            }); 
        }
    }
    window.onSignIn = googleSignIn;
    if(window.gapi && gapi.auth2 && gapi.auth2.getAuthInstance()){
        googleSignIn();

        //gapi.auth2.getAuthInstance().currentUser.get().getBasicProfile() //works
    }
    

    $scope.signOut = function() {
        var auth2 = gapi.auth2.getAuthInstance();
        auth2.signOut().then(function () {
            $scope.googleSignedIn=false;
            if(!$scope.$$phase){
                $scope.$digest();
            }
            ShowHideGoogleSignIn(true);
        });
    }

    function ShowHideGoogleSignIn(show){
        element=window.document.getElementsByClassName("g-signin2").item(0);
        if(show)
            element.style.display='block'
        else
            element.style.display='none'
    }

    coursesService.getHello().then(function (response) {
        //alert(JSON.stringify(response.data));
    });


    this.inputUserId=null;

    //change by the following: get it from server
    $scope.topics=['','Engineering', 'Philosophy', 'History', 'Law', 'Management & Leadership', 'Biology', 'Computer Science', 'Art', 'Data Science', 'Social Sciences', 'Health & Medicine', 'Education & Teaching', 'Environmental Studies', 'Finance', 'Materials Science And Engineering', 'Government', 'Economics', 'Data Analysis & Statistics', 'China', 'Chemistry', 'Political Science', 'Ethics', 'Data Analysis And Statistics', 'Life Sciences', 'Math', 'Mechanical Engineering', 'Global Health', 'Electricity', 'Data Visualization', 'Business & Management', 'Culture', 'Theology', 'Science', 'Statistics & Data Analysis', 'Religion', 'Education', 'Humanities', 'Physics', 'Literature', 'Healthcare', 'Communication', 'Statistics', 'Electronics', 'Health & Society', 'Economics & Finance'];

    $scope.searchCourses = function(){
    if(this.selectedTopic || this.inputUserId){
        $scope.resultCourses=[]
        $scope.isLoading=true;
    }
        if(this.selectedTopic){
            coursesService.getCoursesByTopic(encodeURIComponent(this.selectedTopic)).then(success).catch(function(error) { })
            .finally(finallyAfterCallBack);
        }else if(this.inputUserId){
            coursesService.getCoursesByUser(this.inputUserId).then(success).catch(function(error) { })
            .finally(finallyAfterCallBack);
        }
    }

    function success(response){
        response.data.results.forEach(course => $scope.resultCourses.push(JSON.parse(course)))
    }
    function finallyAfterCallBack(){
        $scope.isLoading = false;
    }

  }]).

  controller('registerController', function($scope, $routeParams, coursesService) {


  });
