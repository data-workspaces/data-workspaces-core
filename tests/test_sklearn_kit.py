import unittest
import sys
from os.path import exists, join

from utils_for_tests import SimpleCase, WS_DIR

try:
    import sklearn
    SKLEARN_INSTALLED=True
except ImportError:
    SKLEARN_INSTALLED=False
try:
    import joblib
    JOBLIB_INSTALLED=True
except ImportError:
    JOBLIB_INSTALLED=False

class TestSklearnKit(SimpleCase):
    def _add_digits_dataset(self):
        self._run_git(['clone',
                       'https://github.com/jfischer/sklearn-digits-dataset.git'])
        self._run_dws(['add','git','--role=source-data','--read-only',
                       './sklearn-digits-dataset'])

    def wrapper_tc(self, model_save_file):
        from sklearn.svm import SVC
        from sklearn.model_selection import train_test_split
        import dataworkspaces.kits.scikit_learn as skkit
        self._setup_initial_repo(git_resources='code,results')
        self._add_digits_dataset()
        dataset = skkit.load_dataset_from_resource('sklearn-digits-dataset',
                                                   workspace_dir=WS_DIR)
        X_train, X_test, y_train, y_test = train_test_split(
            dataset.data, dataset.target, test_size=0.5, shuffle=False)
        classifier = skkit.LineagePredictor(SVC(gamma=0.001),
                                            'multiclass_classification',
                                            input_resource=dataset.resource,
                                            model_save_file=model_save_file,
                                            workspace_dir=WS_DIR,
                                            verbose=False)
        classifier.fit(X_train, y_train)
        score = classifier.score(X_test, y_test)
        self.assertAlmostEqual(score, 0.9688, 3,
                               "Score of %s not almost equal to 0.9688" % score)
        results_dir = join(WS_DIR, 'results')
        results_file = join(results_dir, 'results.json')
        self.assertTrue(exists(results_file))
        save_file = join(results_dir, model_save_file)
        self.assertTrue(exists(save_file))

        # test reloading the trained model
        classifier2 = joblib.load(save_file)
        score2 = classifier.score(X_test, y_test)
        self.assertAlmostEqual(score, 0.9688, 3,
                               "Score of %s not almost equal to 0.9688" % score)

    @unittest.skipUnless(SKLEARN_INSTALLED, "SKIP: Sklearn not available")
    @unittest.skipUnless(JOBLIB_INSTALLED, "SKIP: joblib not available")
    def test_wrapper(self):
        self.wrapper_tc('digits.joblib')


if __name__ == '__main__':
    unittest.main()

