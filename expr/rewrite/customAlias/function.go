package customAlias

import (
	"fmt"
	"strings"
	"text/template"

	"github.com/go-graphite/carbonapi/expr/helper"
	"github.com/go-graphite/carbonapi/expr/interfaces"
	"github.com/go-graphite/carbonapi/expr/types"
	"github.com/go-graphite/carbonapi/pkg/parser"
	"github.com/lomik/zapwriter"
	"github.com/spf13/viper"
	"go.uber.org/zap"
)

func GetOrder() interfaces.Order {
	return interfaces.Any
}

type customAlias struct {
	interfaces.FunctionBase

	configuredAliases map[string]aliasConfig
	description       map[string]types.FunctionDescription
}

type arguments struct {
	Name     string
	Required bool
	Type     types.FunctionType // TODO(civil): convert to enum
}

type aliasConfig struct {
	PreserveName bool
	Template     string
	Args         []arguments
	Module       string
	Group        string
	Description  string

	parsedTemplate *template.Template
}

type customAliasConfig struct {
	Enabled bool
	Aliases map[string]aliasConfig
}

func New(configFile string) []interfaces.RewriteFunctionMetadata {
	logger := zapwriter.Logger("functionInit").With(zap.String("function", "customAlias"))
	if configFile == "" {
		logger.Info("no config file specified",
			zap.String("message", "this function requires config file to work properly"),
		)
		return nil
	}
	v := viper.New()
	v.SetConfigFile(configFile)
	err := v.ReadInConfig()
	if err != nil {
		logger.Fatal("failed to read config file",
			zap.Error(err),
		)
	}

	cfg := customAliasConfig{
		Enabled: false,
	}
	err = v.Unmarshal(&cfg)
	if err != nil {
		logger.Fatal("failed to parse config",
			zap.Error(err),
		)
	}

	if !cfg.Enabled {
		logger.Info("config file found, but function is disabled")
		return nil
	}

	if len(cfg.Aliases) == 0 {
		logger.Info("config file found, but no custom aliases configured")
		return nil
	}

	res := make([]interfaces.RewriteFunctionMetadata, 0)
	f := &customAlias{}
	for n, d := range cfg.Aliases {
		res = append(res, interfaces.RewriteFunctionMetadata{Name: n, F: f})
		if d.Group == "" {
			d.Group = "CustomAlias"
		}
		if d.Module == "" {
			d.Module = "graphite.render.functions"
		}
		if d.Description == "" {
			d.Description = d.Template
		}

		d.parsedTemplate, err = template.New(n).Parse(d.Template)
		if err != nil {
			logger.Warn("failed to parse template for alias",
				zap.String("alias", n),
				zap.Error(err),
			)
			// TODO: Maybe we should hard-fail here?
			continue
		}

		params := make([]types.FunctionParam, 0)
		function := strings.Builder{}
		function.WriteString(n + "(")
		function.WriteString(d.Args[0].Name)
		params = append(params, types.FunctionParam{
			Name:     d.Args[0].Name,
			Required: d.Args[0].Required,
			Type:     d.Args[0].Type,
		})
		for _, p := range d.Args[1:] {
			function.WriteString(", " + p.Name)
			params = append(params, types.FunctionParam{
				Name:     p.Name,
				Required: p.Required,
				Type:     p.Type,
			})
		}
		function.WriteRune(')')

		f.description[n] = types.FunctionDescription{
			Name:        n,
			Params:      params,
			Description: d.Description,
			Module:      d.Module,
			Group:       d.Group,
		}
	}
	return res
}

func (f *customAlias) Do(e parser.Expr, from, until int64, values map[parser.MetricRequest][]*types.MetricData) (bool, []string, error) {
	args, err := helper.GetSeriesArg(e.Args()[0], from, until, values)
	if err != nil {
		return false, nil, err
	}

	aliasCfg := f.configuredAliases[e.Target()]

	field, err := e.GetIntArg(1)
	if err != nil {
		return false, nil, err
	}

	callback, err := e.GetStringArg(2)
	if err != nil {
		return false, nil, err
	}

	var newName string
	if len(e.Args()) == 4 {
		newName, err = e.GetStringArg(3)
		if err != nil {
			return false, nil, err
		}
	}

	var rv []string
	for _, a := range args {
		metric := helper.ExtractMetric(a.Name)
		nodes := strings.Split(metric, ".")
		node := strings.Join(nodes[0:field], ".")
		newTarget := strings.Replace(callback, "%", node, -1)

		if newName != "" {
			newTarget = fmt.Sprintf("alias(%s,\"%s\")", newTarget, strings.Replace(newName, "%", node, -1))
		}
		rv = append(rv, newTarget)
	}
	return true, rv, nil
}

// Description is auto-generated description, based on output of https://github.com/graphite-project/graphite-web
func (f *customAlias) Description() map[string]types.FunctionDescription {
	return f.description
}
