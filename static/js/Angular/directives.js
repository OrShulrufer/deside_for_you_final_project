'use strict';

/* Directives */


angular.module('myApp.directives', []).
  directive('appVersion', ['version', function(version) {
    return function(scope, elm, attrs) {
      elm.text(version);
    };
  }]).
  directive('userForm', function(coursesService) {
        return {
          restrict:'E',
          transclude : true,
          scope: {
            login: '=',
            signup: '=',
            profile: '=',
            user: '=',
            action: '&',
            cancel: '&'
          },
          templateUrl:'partials/templates/userTemplate.html',
          link: function(scope, element, attrs, controllers, transclude) {
            scope.doAction=function(){
              scope.action()();
            }

            scope.$watch('user.user_courses.length', function (newvalue, oldvalue) {
                if(scope.user.user_courses && scope.courseCopy ){
                  refreshCourseComboList();
                }
            });

            function refreshCourseComboList() {
              scope.courses = scope.courseCopy.filter(
                function(e) {
                  return this.indexOf(e.course_number) < 0;
                },
                scope.user.user_courses
              );
            }

            scope.addCourse = function(){
              if(scope.selectedCourse) {
                scope.user.user_courses.push(scope.selectedCourse.course_number);
                $('#collapseCoursesTbl').collapse("show");
                /*
                scope.courses = scope.courses.filter(function(course){
                  return course.course_number != scope.selectedCourse.course_number
                });
                */
              }
            }

            scope.removeCourse = function(removedCourse){
              scope.user.user_courses = scope.user.user_courses.filter(function(course){
                  return course != removedCourse.course
              });
                //scope.courses.push(scope.getCourse(removedCourse.course));
              }
              scope.getCourse = function(course){
                if(!scope.courseCopy)
                  return "";
                return scope.courseCopy.find(c => c.course_number==course);
              }

              if(scope.profile){
                scope.actionText="Update profile";
                 coursesService.getCourses().then(function (response) {
                   scope.courses = response.data.results;
                   scope.courseCopy=[...scope.courses];
                });
                coursesService.getCountries().then(function (response) {
                  scope.countries = response.data.results;
                  scope.countries.unshift({name:"--choose--",id:null})
                });
                scope.educations=[
                  {Name:'--choose education--',Value:null},
                  {Name:'Less than Secondary',Value:0},
                  {Name:'Secondary',Value:0.25},
                  {Name:'Bachelor\'s',Value:0.5},
                  {Name:'Master\'s',Value:0.75},
                  {Name:'Doctorate',Value:1}
                ]
                
                scope.yobs=["--Year--"];
                  for(let i=1930;i<=2010;i++){
                    scope.yobs.push(i);
                  }
              }else if(scope.login){
                scope.actionText="Login";
              }else{
                scope.actionText="Sign Up";
              }

              scope.$watch('user', function (newvalue, oldvalue) {
                  if(scope.user){
                    scope.user.user_courses = scope.user.user_courses || []; 
                    if(!scope.user.country_id)
                      scope.user.country_id=null;
                    if(!scope.user.education && scope.user.education!==0)
                      scope.user.education=null;
                    if(!scope.user.YOB)
                      scope.user.YOB=scope.yobs[0];
                  }
              });

              scope.triggerGoogleSign = function(){
                $(document).ready(function(){
                  $("#google").trigger("click");
                });
              }
          }
      };
  }).
  directive('userSign', function(coursesService) {
    return {
      restrict:'E',
      transclude : true,
      scope: {
        user: '=',
        errormessage: '=',
        login: '&',
        register: '&'
      },
      templateUrl:'partials/templates/signTemplate.html',
      link: function(scope, element, attrs, controllers, transclude) {
        scope.logIn = function(){
            scope.login()();
        }
        scope.signUp = function(){
          scope.register()();
        }

        scope.toggleSignInUp = function(e){
          e.preventDefault();
          $('#logreg-forms .form-signin').toggle(); // display:block or none
          $('#logreg-forms .form-signup').toggle(); // display:block or none
          scope.errormessage="";
        }

      }
    };
  }).
  directive('courseDetails', function(coursesService) {
    return {
      restrict:'E',
      transclude : true,
      scope: {
        course: '='
      },
      templateUrl:'partials/templates/courseTemplate.html',
      link: function(scope, element, attrs, controllers, transclude) {
        scope.isNullOrUndefined = isNullOrUndefined;
      }
    };
  }).
  directive('addCourse', function(coursesService) {
    return {
      restrict:'E',
      transclude : true,
      scope: {
        userid: '='  //is really needed?
      },
      templateUrl:'partials/templates/addCourseTemplate.html',
      link: function(scope, element, attrs, controllers, transclude) {
        scope.newCourse={}
        scope.newCourseTopics = []; 

        coursesService.getTopics().then(function (response) {
            scope.topicList=response.data.results;
        });
        scope.addTopic = function(){
          if(scope.selectedTopic) {
            if(!scope.newCourseTopics.includes(scope.selectedTopic.trim())){
              scope.newCourseTopics.push(scope.selectedTopic);
              scope.topicList = scope.topicList.filter(function(topic){
                return topic != scope.selectedTopic
              });
            }
          }
        }
        scope.removeTopic = function(item){
          scope.newCourseTopics = scope.newCourseTopics.filter(function(topic){
                return topic != item.topic  
          });
          scope.topicList.push(item.topic);
        }

        function setTopicsStr(){
          return scope.newCourse.topicsStr=scope.newCourseTopics.join(",");
        }

        scope.checkUrl = function() {
          if(validURL(scope.newCourse.web_link))
            scope.urlErrorMessage = "";
          else
            scope.urlErrorMessage = "uncorrect link format";
        }

        scope.addNewCourse=function(){
          scope.errorMsg="";
          setTopicsStr();
          if(!scope.newCourse.topicsStr || scope.newCourse.topicsStr.length > 100)
            scope.errorMsg = "must add topics - no longer than total 100 characters";
          else if(!scope.newCourse.title || scope.newCourse.title.length>150 )
            scope.errorMsg ="must enter title no longer than 150 characters";
          else if(!scope.newCourse.description || scope.newCourse.description.length>5000)
            scope.errorMsg ="must enter description no longer than 5000 characters";
          else if(!scope.newCourse.web_link || !validURL(scope.newCourse.web_link) || scope.newCourse.web_link.length > 100)
            scope.errorMsg ="must enter correct course web link";
          else if(!scope.newCourse.institution || scope.newCourse.institution.length > 10)
            scope.errorMsg ="must enter institution name no longer than 10 characters";
          else{
            coursesService.addNewCourse(scope.newCourse).then(function(response){
              $('#userModal').modal('hide');
            });
          }
          
          
        }

      }
    };
  });;

  