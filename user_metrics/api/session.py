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

    from flask.ext.login import LoginManager, current_user, UserMixin, \
        AnonymousUser, confirm_login

    class APIUser(UserMixin):
        """
            Extends USerMixin.  User class for flask-login.
        """
        def __init__(self, name, id, active=True):
            self.name = name
            self.id = id
            self.active = active

        def is_active(self):
            return self.active

        @staticmethod
        def get(uid):
            """
                Used by ``load_user`` to retrieve user session info.
            """
            usr_ref = query_mod.get_api_user(uid)
            if usr_ref:
                try:
                    return APIUser(unicode(str(usr_ref[0])),
                                   int(usr_ref[1]))
                except (KeyError, ValueError):
                    logging.error(__name__ + ' :: Could not get API '
                                             'user info.')
                    return None
            else:
                return None

    class Anonymous(AnonymousUser):
        name = u'Anonymous'

    login_manager = LoginManager()

    login_manager.anonymous_user = Anonymous
    login_manager.login_view = 'login'
    login_manager.login_message = u'Please log in to access this page.'
    login_manager.refresh_view = 'reauth'

    @login_manager.user_loader
    def load_user(uid):
        return APIUser.get(int(uid))
