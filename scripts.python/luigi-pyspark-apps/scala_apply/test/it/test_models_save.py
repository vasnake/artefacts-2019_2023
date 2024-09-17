import os
import pytest

from prj.adapters.dataset import LookalikeLearnsetAdapter
from prj.adapters.model.lal import LookalikeMultinomialNB, LookalikeSGDClassifier

from prj.interface.models.test.conftest import make_lal_learning_data


class TestLearnSaveModel(object):

    @pytest.mark.parametrize("model_class", [LookalikeMultinomialNB, LookalikeSGDClassifier])
    def test_save_sa_compatible(self, model_class, tmpdir, make_lal_learning_data):
        train, test, unlabeled, grouped_features = make_lal_learning_data(uid_type_names=["HID"], random_state=0)
        learnset = LookalikeLearnsetAdapter().set_data(train, grouped_features, label_names=["score", "category"])

        model = model_class().learn(learnset)
        assert model.is_sa_compatible

        model.save(tmpdir.strpath)
        assert os.path.isfile(os.path.join(tmpdir.strpath, "model_repr.json"))
