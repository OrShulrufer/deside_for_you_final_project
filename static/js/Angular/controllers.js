angular.module('App.controllers', []).
    
  /* initall controller  */
  controller('userController', ['$rootScope','$routeParams','coursesService', function ($scope,$routeParams, coursesService) {
    var emailCookie="DFYemail";
    var passwordCookie="DFYpassword";
    var usernameCookie="DFYuser_name";

    $('#cover-spin').show(0); 
    setTimeout(function(){ $('#cover-spin').hide();}, 2000);

    setTimeout(function(){
       $('#userModal').on('hidden.bs.modal', function (e) {
        if($scope.needToCompleteProfile)
            $('#userModal').modal('show');
      })
    }, 1000);

    $scope.ucModel={};

    var signingUp=false;
    var logingIn=false;
    
    //$('#cover-spin').show(0); //$('#cover-spin').hide();
    setRegularSignedInProps(true);
    
    $scope.user={};
    function setUser(emailValue,passwordValue){
        $scope.user={'email':emailValue, 'password':passwordValue};
    }
    function setUserCopy(){
        $scope.userCopy = jQuery.extend(true, {}, $scope.user);
    }
    setUserCopy();

    function googleSignIn (someParam) {
        if($scope.regularSignedIn)
            return;
        $scope.googleAuthInstance = gapi.auth2.getAuthInstance()
        if($scope.googleAuthInstance.currentUser.get().isSignedIn()) {
            $('#cover-spin').show(0);
            ShowHideGoogleSignIn(false);
            $scope.googleSignedIn=true;
            /*
            $scope.userName=$scope.googleAuthInstance.currentUser.get().getBasicProfile().Ad;
            if(!$scope.$$phase) {
                $scope.$digest();
              }
              */
             
            var id_token = $scope.googleAuthInstance.currentUser.get().getAuthResponse().id_token;
            coursesService.validateGoogleUser(id_token).then(function (response) {
                $('#signModal').modal('hide');
                afterGettingUser(response);
            }).catch(function(error) { })
            .finally(function(){$('#cover-spin').hide();}); 
        }
    }
    window.onSignIn = googleSignIn;
    if(window.gapi && gapi.auth2 && gapi.auth2.getAuthInstance()){
        googleSignIn();

        //gapi.auth2.getAuthInstance().currentUser.get().getBasicProfile() //works
    }
    

    $scope.signOut = function() {
        if($scope.regularSignedIn){
            $scope.regularSignedIn=false;
            deleteCookie(usernameCookie);
            deleteCookie(emailCookie);
            deleteCookie(passwordCookie);
            //ShowHideGoogleSignIn(true);
        }else{
            var auth2 = gapi.auth2.getAuthInstance();
            auth2.signOut().then(function () {
                $scope.googleSignedIn=false;
                if(!$scope.$$phase){
                    $scope.$digest();
                }
                //ShowHideGoogleSignIn(true);
            });
        }
        $scope.user=null;
        $scope.needToCompleteProfile=false;
    }

    function ShowHideGoogleSignIn(show){
        element=window.document.getElementsByClassName("g-signin2").item(0);
        if(show)
            element.style.display='block'
        else
            element.style.display='none'
    }

    $scope.prepareNormalSignUp = function (){
        signingUp=true;
        logingIn=false;
        setUser("","");
        $scope.showSignUp=true;
        $scope.showLogin=false;
        $scope.modalTitle="Welcome!"
    }  
    $scope.prepareNormalSignIn = function (){
        logingIn=true;
        signingUp=false;
        setUser("","");
        $scope.showLogin=true;
        $scope.showSignUp=false;
        $scope.modalTitle="Welcome!"
    }

    function checkSignInUp () {
        $scope.ucModel.signInUpError="";
        if($scope.user.userid_DI)
            return true;
        if(!$scope.user.email.includes("MHxPC") && !validateEmail($scope.user.email)){
            $scope.ucModel.signInUpError = "email is in wrong format";
            return false;
        }else if(String($scope.user.password).trim().length==0) {
            $scope.ucModel.signInUpError="must provide password no longer than 45 notes";
            return false;
        }
        return true;
    }

    $scope.signInOrGetError = function() {
        logingIn=true;
        signingUp=false;
        if(checkSignInUp()){
            $('#cover-spin').show(0);
            coursesService.validateRegularSignInUp(true, 
                $scope.user.userid_DI || $scope.user.email.trim(), 
                $scope.user.password.trim() || "hi", 
                $scope.user.user_name || null).
            then(successSignInUpResponse).
            catch(function(error) { $scope.ucModel.signInUpError = error.message })
            .finally(finallySignInUpResponse);
        }
    }
    $scope.signUpOrGetError = function() {
        signingUp=true;
        logingIn=false;
        if(checkSignInUp()){
            var password = $scope.user.password;
            if($scope.user.user_name.length > 65)
                $scope.ucModel.signInUpError="user name must be less than 65 characters";
            else if($scope.user.email.length > 45)
                $scope.ucModel.signInUpError="email must be less than 45 characters";
            else if(!password || (password.length < 8 && password.length > 45)){
                $scope.ucModel.signInUpError="password must be at least 8 characters and less than 45";
            }else if(password!==$scope.user.repeatPassword){
                $scope.ucModel.signInUpError="password repeat field must be the same as password field";
            }
            else{
                $('#cover-spin').show(0);
                coursesService.validateRegularSignInUp(false,
                    $scope.user.email, $scope.user.password,$scope.user.user_name || null).
                then(successSignInUpResponse).
                catch(function(error) { $scope.ucModel.signInUpError = error.message })
                .finally(finallySignInUpResponse);
            }
        }
    }

    function getUser(email)  {
        coursesService.getUser(email).
            then(function(response){
                afterGettingUser(response);
            }).
            catch(function(error) { })
            .finally(function(){});
    }
    function afterGettingUser(response) {
        $scope.user = response.data;
        setUserCopy();
        setUserName();
        checkUserProfile();
    }
    
    var hasNeededCompleteProfile=false;
    function checkUserProfile() {
        $scope.needToCompleteProfile =($scope.regularSignedIn || $scope.googleSignedIn) &&
        !$scope.user.is_course_provider &&
        !$scope.user.education &&  $scope.user.education!==0
        if($scope.needToCompleteProfile) {
            $scope.modalTitle="To Complete registeration you must fill profile details"
            setTimeout(function(){ $('#userModal').modal('show');}, 500);
            hasNeededCompleteProfile=true;
        }else if(hasNeededCompleteProfile){
            //hide update modal when register/profile is completed
            $('#userModal').modal('hide');
            hasNeededCompleteProfile=false;
        }
    }

    $scope.updateProfile = function() {
        $scope.UserModalErrorMsg="";
        var user = $scope.userCopy;
        user.updateDemographic=false;
        if(!user.user_name && user_name.length > 65)
            $scope.UserModalErrorMsg="user name must be less than 65 characters";
        else if(!user.gender && user.gender!==0){
            $scope.UserModalErrorMsg="must select gender";
        }
        else if(!user.country_id) {
            $scope.UserModalErrorMsg="must select country";
        }
        else if(!user.YOB || isNaN(user.YOB)) {
            $scope.UserModalErrorMsg="must select year of birth";
        }
        else if(!user.education && user.education!==0) {
            $scope.UserModalErrorMsg="must select education";
        }
        else if(!user.user_courses || user.user_courses.length==0) {
            $scope.UserModalErrorMsg="must add at least 1 course you participated in";
        }
        else {
            if(user.gender!=$scope.user.gender || user.country_id!=$scope.user.country_id ||
                user.YOB!=$scope.user.YOB || user.education!=$scope.user.education)
                user.updateDemographic=true;
            
            $("#cover-spin-header").html("Please wait for the user updating");
            $('#cover-spin').show(0);
            coursesService.updateUser(user).then(function(response){
                //alert(response.data);
                getUser(user.email);
                $('#cover-spin').hide(0);
                $("#cover-spin-header").html("");
            });
        }
    }
    function successSignInUpResponse(response) {
        var user = response.data;
            if(user.response_message){
                $scope.ucModel.signInUpError = user.response_message;
            }else{
                $scope.user = user;
                setCookie(usernameCookie,user.user_name || '',1);
                setCookie(emailCookie,user.email,1);
                setCookie(passwordCookie,user.password,1)
                //if(logingIn)
                    $('#signModal').modal('hide');
                setRegularSignedInProps();
            }
    }
    function finallySignInUpResponse(){
        $('#cover-spin').hide();
    }

    function setUserName(){
        if($scope.user) {
            $scope.userName= $scope.user.user_name || $scope.user.email;
            if(!$scope.$$phase) {
                $scope.$digest();
            }
        }
    }

    function setRegularSignedInProps(onInit){
        if(getCookie(emailCookie)){
            $scope.regularSignedIn=true;
            setUserName();
            ShowHideGoogleSignIn(false);
            setUserCopy();
            if(onInit){
                getUser(getCookie(emailCookie));
            }else{
                checkUserProfile();
            }
        } 
    }

    $scope.restoreUserCopy = function(){
        setUserCopy();
    }

  }]).

  controller('homeController', function($scope, $routeParams, coursesService) {
    coursesService.getHello().then(function (response) {
        //alert(JSON.stringify(response.data));
    });

    this.inputUserId=null;
    $scope.homeLogin = $scope.signInOrGetError//$scope.signIn;
    $scope.homeRegister = $scope.signUpOrGetError//$scope.signUp;
    $scope.homeUpdateProfile = $scope.updateProfile
    

    coursesService.getTopics().then(function (response) {
        $scope.topics=response.data.results;
    });
    //$scope.topics=['','Engineering', 'Philosophy', 'History', 'Law', 'Management & Leadership', 'Biology', 'Computer Science', 'Art', 'Data Science', 'Social Sciences', 'Health & Medicine', 'Education & Teaching', 'Environmental Studies', 'Finance', 'Materials Science And Engineering', 'Government', 'Economics', 'Data Analysis & Statistics', 'China', 'Chemistry', 'Political Science', 'Ethics', 'Data Analysis And Statistics', 'Life Sciences', 'Math', 'Mechanical Engineering', 'Global Health', 'Electricity', 'Data Visualization', 'Business & Management', 'Culture', 'Theology', 'Science', 'Statistics & Data Analysis', 'Religion', 'Education', 'Humanities', 'Physics', 'Literature', 'Healthcare', 'Communication', 'Statistics', 'Electronics', 'Health & Society', 'Economics & Finance'];

    $scope.searchCourseByUser = function(){
        if(($scope.googleSignedIn || $scope.regularSignedIn) && $scope.user.email){
            $scope.resultCourses=[]
            $scope.isLoading=true;
            coursesService.getCoursesByUser($scope.user.email).then(success).
            catch(function(error) { alert(error) })
            .finally(finallyAfterCallBack);
        }
    }

    $scope.searchCourseByTopic = function(){
        if(this.selectedTopic){
            $scope.resultCourses=[]
            $scope.isLoading=true;
            coursesService.getCoursesByTopic(encodeURIComponent(this.selectedTopic)).then(success).
            catch(function(error) { console.log(error);})
            .finally(finallyAfterCallBack);
        }
    }

    /*
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
    */

    function success(response){
        response.data.results.forEach(course => $scope.resultCourses.push(course))
        var ordNum=1;
        $scope.resultCourses.forEach(course => {course=course;
            course.ordNum = ordNum++;
            var delimiterNumber = 50;
            var countDelimiter = (course.description.match(/\s/g) || []).length;
            if(countDelimiter > delimiterNumber){
                var tokensStart = course.description.split(" ").slice(0,delimiterNumber);
                course.lessDescription = tokensStart.join(" ") ;
                course.descriptionText = course.lessDescription;
            }else
                course.descriptionText = course.description;
        })
        $([document.documentElement, document.body]).animate({
            scrollTop: $("#coursesList").offset().top
        }, 2000);
        $(window).bind("mousewheel", function() {
            $("html, body").stop();
        });
        
    }
    function finallyAfterCallBack(){
        $scope.isLoading = false;
    }

    $scope.toggleMore=function($event,courseElem){
        $event.stopPropagation();
        thisCourse= courseElem.course;
        thisCourse.showMore = !thisCourse.showMore;
        thisCourse.descriptionText = thisCourse.showMore? thisCourse.description : thisCourse.lessDescription;
    }

    $scope.openCourseDetails = function(course){
        var selectedCourse = $scope.resultCourses.filter(function(c){
            return c.course_number == course.course.course_number
        })[0];
        $scope.selectedCourse = allCoursesDetails.filter(function(c){
            return c.course_number == course.course.course_number
        })[0];
        var wwwStart= selectedCourse.web_link.indexOf("www.")
        if(wwwStart > 9 || wwwStart==-1){
            selectedCourse.web_link = "www."+selectedCourse.web_link;
        }
        if($scope.selectedCourse){
            $scope.selectedCourse.course_title=selectedCourse.course_title;
            $scope.selectedCourse.description=selectedCourse.description;
            $scope.selectedCourse.institution=selectedCourse.institution;
            $scope.selectedCourse.web_link=selectedCourse.web_link;
            $scope.selectedCourse.gender = $scope.user.gender;
        }  
        else
            $scope.selectedCourse=selectedCourse;

        $scope.courseModalTitle = selectedCourse.course_title;
        $('#courseModal').modal('show');
    }
    $scope.selectedCourse={}
    var allCoursesDetails;
    coursesService.getCoursesDetails().then(function (response) {
        allCoursesDetails=response.data.results;
    });
    
    
      

  }).

  //maybe use modal
  controller('registerController', function($scope, $routeParams, coursesService) {
        //logout auto;

    })


    