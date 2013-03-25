"""
    This module defines utilizes the flask-login_ package to implement
    session management for the API.

    .. flask-login_: http://pythonhosted.org/Flask-Login/

"""

__author__ = {
    "ryan faulkner": "rfaulkner@wikimedia.org"
}
__date__ = "2013-03-21"
__license__ = "GPL (version 2 or later)"



from user_metrics.config import logging, settings
from user_metrics.metrics import query_mod


# API User Authentication
# #######################


# With the presence of flask.ext.login module
if settings.__flask_login_exists__:

    from flask import escape
    from werkzeug.security import generate_password_hash,\
        check_password_hash

    from flask.ext.login import LoginManager, current_user, UserMixin, \
        AnonymousUser, confirm_login

    class APIUser(UserMixin):
        """
            Extends USerMixin.  User class for flask-login.  Implements a way
            to add user credentials with _HMAC and salting.

            .. HMAC_: http://tinyurl.com/d8zbbem

        """
        def __init__(self, username, authenticated=False):

            self.name = escape(unicode(username))
            self.authenticated = authenticated

            user_ref =  query_mod.get_api_user(username, by_id=False)
            if user_ref:
                self.id = unicode(user_ref[1])
                self.active = True
                self.pw_hash = unicode(str(user_ref[2]))
            else:
                self.id = None
                self.active = False
                self.pw_hash = None

            logging.debug(__name__ + ' :: Initiatializing user obj. '
                                     'user: "{0}", '
                                     'is active: "{1}", '
                                     'is auth: {2}'.
                format(username, self.active, self.authenticated))

        def is_active(self):
            return self.active

        def is_authenticated(self):
            return self.authenticated

        def authenticate(self, password):
            password = escape(unicode(password))
            logging.debug(__name__ + ' :: Authenticating "{0}"/"{1}" '
                                     'on hash "{2}" ...'.
                format(self.name, password, self.pw_hash))
            if self.check_password(password):
                self.authenticated = True
            else:
                self.authenticated = False

        @staticmethod
        def get(uid):
            """
                Used by ``load_user`` to retrieve user session info.
            """
            user_ref =  query_mod.get_api_user(uid)
            if user_ref:
                return APIUser(str(user_ref[0]),
                               authenticated=True)
            else:
                return None

        def set_password(self, password):
            try:
                password = escape(unicode(password))
                self.pw_hash = generate_password_hash(password)
            except (TypeError, NameError) as e:
                logging.error(__name__ + ' :: Hash set error - ' + e.message)
                self.pw_hash = None

        def check_password(self, password):
            if self.pw_hash:
                try:
                    password = escape(unicode(password))
                    return check_password_hash(self.pw_hash, password)
                except (TypeError, NameError) as e:
                    logging.error(__name__ +
                                  ' :: Hash check error - ' + e.message)
                    return False
            else:
                return False

        def register_user(self):
            """ Writes the user credentials to the datastore. """

            # 1. Only users not already registered
            # 2. Ensure that the user is unique
            # 3. Write the user / pass to the db

            if not self.active:
                if not query_mod.get_api_user(self.name, by_id=False):
                    query_mod.insert_api_user(self.name, self.pw_hash)
                    logging.debug(__name__ + ' :: Added user {0}'.
                        format(self.name))
                else:
                    logging.error(__name__ + 'Could not add user {0}'.
                        format(self.name))
                self.active = True

    class Anonymous(AnonymousUser):
        name = u'Anonymous'

    login_manager = LoginManager()

    login_manager.anonymous_user = Anonymous
    login_manager.login_view = 'login'
    login_manager.login_message = u'Please log in to access this page.'
    login_manager.refresh_view = 'reauth'

    @login_manager.user_loader
    def load_user(uid):
        return APIUser.get(uid)
