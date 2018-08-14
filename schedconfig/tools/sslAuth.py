import logging
from django.conf import settings
from django.contrib.auth import login, authenticate
from django.core.exceptions import ImproperlyConfigured

try:
    from django.utils.deprecation import MiddlewareMixin  # Django 1.10+
except ImportError:
    MiddlewareMixin = object  # Django < 1.10
from django.http import HttpResponseRedirect
from django.shortcuts import resolve_url
from importlib import import_module

try:
    from django.contrib.auth import get_user_model

    User = get_user_model()
except ImportError:
    from django.contrib.auth.models import User

logging.basicConfig()
logger = logging.getLogger(__name__)

class SSLClientAuthBackend(object):
    @staticmethod
    def authenticate(request=None):
        _module_name, _function_name = settings.USER_DATA_FN.rsplit('.', 1)
        _module = import_module(_module_name)  # We need a non-empty fromlist
        USER_DATA_FN = getattr(_module, _function_name)
        if not request.is_secure():
            logger.debug("insecure request")
            return None
        authentication_status = request.META.get('SSL_CLIENT_VERIFY', None)
        if (authentication_status != "SUCCESS" or 'SSL_CLIENT_S_DN' not in request.META):
            logger.warn(
                "HTTP_X_SSL_AUTHENTICATED marked failed or "
                "HTTP_X_SSL_USER_DN "
                "header missing")
            return None
        dn = request.META.get('SSL_CLIENT_S_DN')
        user_data = USER_DATA_FN(dn)
        username = user_data['username']
        try:
            user = User.objects.get(username=username)
        except User.DoesNotExist:
            logger.info("user {0} not found".format(username))
            if settings.AUTOCREATE_VALID_SSL_USERS:
                user = User(**user_data)
                user.save()
            else:
                return None
        logger.info("user {0} authenticated using a certificate issued to "
                    "{1}".format(username, dn))
        return user

    def get_user(self, user_id):
        try:
            return User.objects.get(pk=user_id)
        except User.DoesNotExist:
            return None


class SSLClientAuthMiddleware(MiddlewareMixin):
    def process_request(self, request):
        if not hasattr(request, 'user'):
            raise ImproperlyConfigured()
        if request.user.is_authenticated():
            return
        if int(request.META.get('HTTP_X_REST_API', 0)):
            user = authenticate(request=request)
            if user is None or not user.is_authenticated():
                return
            logger.debug("REST API call, not logging user in")
            request.user = user
        elif request.path_info == settings.LOGIN_URL:
            user = authenticate(request=request)
            if user is None or not user.is_authenticated():
                return
            logger.info("Logging user in")
            login(request, user)
            return HttpResponseRedirect(resolve_url(settings.LOGIN_REDIRECT_URL))
